package com.orientechnologies.orient.server.distributed;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.orientechnologies.common.concur.lock.OLockManager;
import com.orientechnologies.common.util.MersenneTwister;
import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.storage.ORecordDuplicatedException;

/**
 * @author Andrey Lomakin
 * @since 17.08.12
 */
public final class OLocalDHTNode implements ODHTNode {
  private static final int                   MAX_RETRIES           = 10;
  private static final int                   MAX_RECORDS_TO_RETURN = 64;

  private final MersenneTwister              random                = new MersenneTwister();

  private final long                         nodeId;

  private final AtomicLongArray              fingerPoints          = new AtomicLongArray(63);
  private AtomicLong                         predecessor           = new AtomicLong(-1);

  private final NavigableMap<Long, Record>   db                    = new ConcurrentSkipListMap<Long, Record>();

  private volatile ODHTNodeLookup            nodeLookup;

  private AtomicInteger                      next                  = new AtomicInteger(1);
  private final OLockManager<Long, Runnable> lockManager           = new OLockManager<Long, Runnable>(true, 500);
  private volatile long[]                    successorsList        = new long[0];

  private final ExecutorService              gmExecutorService;
  private final ExecutorService              lmExecutorService;
  private final ExecutorService              readRepairService;

  private volatile NodeState                 state;

  private final OMerkleTree                  merkleTree            = new OMerkleTree(db);

  private final int                          replicaCount;
  private final int                          syncReplicaCount;

  private volatile int                       size;

  public OLocalDHTNode(long nodeId, int replicaCount, int syncReplicaCount) {
    this.nodeId = nodeId;
    for (int i = 0; i < fingerPoints.length(); i++)
      fingerPoints.set(i, -1);

    this.replicaCount = replicaCount;
    this.syncReplicaCount = syncReplicaCount;

    gmExecutorService = Executors.newSingleThreadExecutor(new GlobalMaintenanceProtocolThreadFactory(nodeId));
    lmExecutorService = Executors.newSingleThreadExecutor(new LocalMaintenanceProtocolThreadFactory(nodeId));

    readRepairService = new ThreadPoolExecutor(0, Runtime.getRuntime().availableProcessors() / 2, 60L, TimeUnit.SECONDS,
        new ArrayBlockingQueue<Runnable>(256), new ReadRepairThreadFactory(nodeId), new ThreadPoolExecutor.CallerRunsPolicy());
  }

  public ODHTNodeLookup getNodeLookup() {
    return nodeLookup;
  }

  public void setNodeLookup(ODHTNodeLookup nodeLookup) {
    this.nodeLookup = nodeLookup;
  }

  public void createDHT() {
    Logger logger = LoggerFactory.getLogger(OLocalDHTNode.class);
    logger.info("New ring creation was started");

    predecessor.set(-1);
    fingerPoints.set(0, nodeId);
    state = NodeState.PRODUCTION;

    gmExecutorService.submit(new GlobalMaintenanceProtocol());
    // lmExecutorService.submit(new LocalMaintenanceProtocol());

    logger.info("New ring was created");
  }

  public long getNodeId() {
    return nodeId;
  }

  public boolean joinDHT(long nodeId) {
    Logger logger = LoggerFactory.getLogger(OLocalDHTNode.class);

    try {
      logger.info("Join is started using node with id {}", nodeId);

      final ODHTNode node = nodeLookup.findById(nodeId);
      if (node == null) {
        logger.error("Node with id {} is absent.", nodeId);
        return false;
      }

      if (state == null) {
        gmExecutorService.submit(new GlobalMaintenanceProtocol());
        // lmExecutorService.submit(new LocalMaintenanceProtocol());
      }

      random.setSeed((new Random()).nextLong());

      state = NodeState.JOIN;

      predecessor.set(-1);
      int retryCount = 0;

      while (true) {
        try {
          logger.info("Wait till node {} will be ready to process requests.", nodeId);
          while (node.state() == null)
            Thread.sleep(100);
          logger.info("Node {} is ready to process requests", nodeId);

          final long successorId = node.findSuccessor(this.nodeId);
          fingerPoints.set(0, successorId);

          ODHTNode successor = nodeLookup.findById(successorId);
          if (successor == null) {
            if (retryCount < MAX_RETRIES) {
              logger.debug("Node {} is offline, retry {}-d time.", successorId, retryCount);
              retryCount++;
              Thread.sleep(100);
            } else {
              logger.error("Node " + successorId + " is offline, max retries is reached");
              return false;
            }

            continue;
          }

          final long prevPredecessor = successor.notifyParent(this.nodeId);
          if (prevPredecessor > -1) {
            final ODHTNode prevPredecessorNode = nodeLookup.findById(prevPredecessor);
            if (prevPredecessorNode != null)
              try {
                prevPredecessorNode.requestStabilization();
              } catch (ONodeOfflineException e) {
                // ignore
              }
          }

          logger.info("Join completed, successor is {}", fingerPoints.get(0));

          return true;
        } catch (ONodeOfflineException ooe) {
          if (ooe.getNodeId() == nodeId) {
            logger.error("Node with id " + nodeId + " is absent.");
            return false;
          }

          if (retryCount < MAX_RETRIES) {
            logger.debug("Node {} is offline, retry {}-d time.", ooe.getNodeId(), retryCount);
            retryCount++;
            Thread.sleep(100);
          } else {
            logger.error("Node {} is offline, max retries is reached", ooe.getNodeId());
            return false;
          }
        }
      }

    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
      return false;
    }
  }

  public long findSuccessor(long key) {
    Logger logger = LoggerFactory.getLogger(this.getClass().getName() + ".findSuccessor");

    while (true) {
      logger.debug("Successor request for key {}", key);
      final long successorId = fingerPoints.get(0);

      if (insideInterval(nodeId, successorId, key, true)) {
        logger.debug("Key {} inside interval {} - {} ", new Object[] { key, nodeId, successorId });
        return successorId;
      }

      long nodeId = findClosestPrecedingFinger(key);

      logger.debug("Closest preceding node for key {} is {}", key, nodeId);

      ODHTNode node = nodeLookup.findById(nodeId);
      if (node == null) {
        if (nodeId != successorId) {
          clearFingerPoint(nodeId);
          continue;
        } else {
          final long[] successors = successorsList;
          for (final long successor : successors) {
            if (successor == this.nodeId)
              return this.nodeId;

            final ODHTNode successorNode = nodeLookup.findById(successor);
            if (successorNode != null) {
              try {
                return successorNode.findSuccessor(key);
              } catch (ONodeOfflineException noe) {
                logger.error(noe.toString(), noe);
              }
            }
          }

          throw new ONodeOfflineException("Node " + nodeId + " is offline .", null, nodeId);
        }
      }

      try {
        final long successorResult = node.findSuccessor(key);
        logger.debug("Successor for key {} is {}", key, successorResult);
        return successorResult;
      } catch (ONodeOfflineException e) {
        if (nodeId != successorId) {
          clearFingerPoint(nodeId);
        } else {
          final long[] successors = successorsList;
          for (final long successor : successors) {
            if (successor == this.nodeId)
              return this.nodeId;

            final ODHTNode successorNode = nodeLookup.findById(successor);
            if (successorNode != null) {
              try {
                return successorNode.findSuccessor(key);
              } catch (ONodeOfflineException noe) {
                logger.error(noe.toString(), noe);
              }
            }
          }
          throw e;
        }
      }
    }
  }

  private void clearFingerPoint(long nodeId) {
    fingerPointCycle: while (true) {
      for (int i = 0; i < fingerPoints.length(); i++) {
        final long finger = fingerPoints.get(i);
        if (finger > -1 && nodeId == finger)
          if (fingerPoints.compareAndSet(i, finger, -1))
            return;
          else
            continue fingerPointCycle;
      }
      return;
    }
  }

  public long[] getSuccessors() {
    final long successorId = getSuccessor();
    if (successorId == nodeId)
      return new long[0];

    final long[] localSuccessors = successorsList;
    long[] result = new long[localSuccessors.length + 1];
    result[0] = successorId;

    if (localSuccessors.length > 0)
      System.arraycopy(localSuccessors, 0, result, 1, localSuccessors.length);

    return result;
  }

  private long findClosestPrecedingFinger(long key) {
    // log("Closest preceding request for key " + key);

    for (int i = fingerPoints.length() - 1; i >= 0; i--) {
      final long fingerPoint = fingerPoints.get(i);
      if (fingerPoint > -1 && insideInterval(this.nodeId, key, fingerPoint, false)) {
        // log("Closest preceding finger for key " + key + " is " + fingerPoint);
        return fingerPoint;
      }
    }

    // log("Closest preceding finger for key " + key + " is " + this.id);

    return this.nodeId;
  }

  public long getSuccessor() {
    return fingerPoints.get(0);
  }

  public Long getPredecessor() {
    return predecessor.get();
  }

  public Record createRecord(String data) {
    waitTillJoin();

    int retryCount = 0;

    while (true) {
      try {
        final long id = random.nextLong(Long.MAX_VALUE);
        retryCount++;

        return createRecord(id, data);
      } catch (ORecordDuplicatedException e) {
        // ignore
        if (retryCount >= MAX_RETRIES)
          throw e;
      }
    }
  }

  public Record createRecord(long recordId, String data) {
    Logger logger = LoggerFactory.getLogger(this.getClass().getName() + ".createRecord");

    waitTillJoin();

    int retryCount = 0;

    while (true) {
      logger.debug("Looking for successor for record {}", recordId);
      final long successorId = findSuccessor(recordId);

      logger.debug("Successor for record {} is {}", recordId, successorId);
      retryCount++;

      if (successorId == nodeId) {
        return createRecordInNode(recordId, data);
      } else {
        final RemoteNodeCallResult<Record> result = remoteNodeCreate(recordId, data, retryCount, successorId);
        if (result.repeat)
          continue;

        return result.data;
      }
    }
  }

  public Record getRecord(long recordId) {
    Logger logger = LoggerFactory.getLogger(this.getClass().getName() + ".getRecord");

    waitTillJoin();
    int retryCount = 0;

    while (true) {
      retryCount++;

      logger.debug("Looking for successor for node with id {}", recordId);
      final long successorId = findSuccessor(recordId);
      logger.debug("Successor for record {} is {}", recordId, successorId);

      if (successorId != nodeId) {
        final RemoteNodeCallResult<Record> remoteNodeGetResult = remoteNodeGet(recordId, successorId, retryCount);

        if (remoteNodeGetResult.repeat)
          continue;

        return remoteNodeGetResult.data;
      }

      return getRecordFromNode(recordId, true);
    }
  }

  @Override
  public Record createRecordInNode(long recordId, String data) {
    waitTillJoin();

    final Record result = addData(recordId, data);
    replicateRecord(result);

    return result;
  }

  @Override
  public void updateRecordInNode(long recordId, Record record) {
    synchronizeReplicas(recordId, syncReplicaCount);

    updateData(recordId, record);

    replicateRecord(recordId);
    startReadRepair(recordId);
  }

  @Override
  public void deleteRecordFromNode(long recordId, ODHTRecordVersion version) {
    synchronizeReplicas(recordId, syncReplicaCount);

    removeData(recordId, version);

    replicateRecord(recordId);
    startReadRepair(recordId);
  }

  private void synchronizeReplicas(long recordId, int syncReplicaCount) {
    if (syncReplicaCount < 1)
      return;

    Logger logger = LoggerFactory.getLogger(this.getClass().getName() + ".synchronizeReplicas");
    final Record ownerRecord = readData(recordId);

    RecordMetadata primaryMetadata;

    if (ownerRecord == null)
      primaryMetadata = null;
    else
      primaryMetadata = new RecordMetadata(ownerRecord.getId(), ownerRecord.getVersion());

    logger.debug("Start synchronization for record {} replica count is {} primary metadata is {}", new Object[] { recordId,
        syncReplicaCount, primaryMetadata });

    while (true) {
      ODHTNode primaryHolder = this;

      logger.debug("Find replica holders for node {} replica count is {}", nodeId, replicaCount);

      final long[] replicaHolders = getSuccessors();
      logger.debug("Replica holders for node {} are {}", nodeId, replicaHolders);

      if (replicaHolders == null || replicaHolders.length == 0)
        return;

      final Set<Long> holdersToReplicate = chooseReplicas(replicaHolders, syncReplicaCount);

      final Set<Long> replicaHoldersToUpdate = new HashSet<Long>();

      logger.debug("Replica holders for node {} are {}", nodeId, replicaHoldersToUpdate);

      for (long holderId : holdersToReplicate) {
        final ODHTNode holderNode = nodeLookup.findById(holderId);
        if (holderNode == null)
          continue;

        logger.debug("Holder with id {} is absent.", holderId);

        try {
          final RecordMetadata nodeMetadata = holderNode.getRecordMetadataFromNode(recordId);
          logger.debug("Metadata for record id {} from node {} is {}", new Object[] { recordId, holderId, nodeMetadata });

          if (primaryMetadata == null) {
            if (nodeMetadata != null) {
              logger.debug(
                  "Primary metadata is null but node metadata is not so we replace it. record id" + " {} node metadata {}",
                  recordId, nodeMetadata);

              replicaHoldersToUpdate.add(primaryHolder.getNodeId());

              primaryMetadata = nodeMetadata;
              primaryHolder = holderNode;
            }
          } else {
            if (nodeMetadata != null) {
              final int cp = primaryMetadata.getVersion().compareTo(nodeMetadata.getVersion());

              if (cp < 0) {
                replicaHoldersToUpdate.add(primaryHolder.getNodeId());

                primaryMetadata = nodeMetadata;
                primaryHolder = holderNode;

                logger.debug("Primary metadata is not null but node metadata is more up to date  so we replace it."
                    + " record id {} node metadata {} primary metadata {}",
                    new Object[] { recordId, nodeMetadata, primaryMetadata });
              } else {
                logger.debug("Primary metadata is not null but node metadata out of date so we replace it. record id {}"
                    + " node metadata {} primary metadata {}", new Object[] { recordId, nodeMetadata, primaryMetadata });
                replicaHoldersToUpdate.add(holderNode.getNodeId());
              }
            } else {
              logger.debug("Node metadata is null but primary metadata is not so we replace it. record id {}"
                  + " node metadata {} primary metadata {}", new Object[] { recordId, nodeMetadata, primaryMetadata });
              replicaHoldersToUpdate.add(holderNode.getNodeId());
            }
          }

        } catch (Exception e) {
          // ignore
          logger.error("Exception during synchronization of record {} for node {}", recordId, holderId);
        }
      }

      logger.debug("Replica holders to update for record {}", recordId);

      if (!replicaHoldersToUpdate.isEmpty()) {
        logger.debug("Getting record from {} with id {}", primaryHolder.getNodeId(), recordId);

        Record result;
        if (nodeId == primaryHolder.getNodeId())
          result = readData(recordId);
        else
          try {
            result = primaryHolder.getRecordFromNode(recordId, false);
          } catch (Exception e) {
            continue;
          }

        logger.debug("Record with id {} was returned from {}" + " with content {}" + result,
            new Object[] { recordId, primaryHolder.getNodeId(), result });

        for (long replicaHolderId : replicaHoldersToUpdate) {
          ODHTNode replicaHolder = nodeLookup.findById(replicaHolderId);
          if (replicaHolder == null)
            continue;

          logger.debug("Holder with id {}" + " is absent during synchronization of record with id {}", replicaHolderId, recordId);

          try {
            logger.debug("Update of replica with {} for node {}", recordId, replicaHolder.getNodeId());
            replicaHolder.updateReplica(result, false);
            logger.debug("Replica with {} for node {} was updated.", recordId, replicaHolder.getNodeId());
          } catch (Exception e) {
            logger.error("Exception during replication of record with {} for node {}",
                new Object[] { recordId, replicaHolder.getNodeId() }, e);
          }
        }
      }

      logger.debug("Synchronization of record with id {} was completed", recordId);
      return;
    }
  }

  private void startReadRepair(long recordId) {
    readRepairService.submit(new ReadRepairTask(recordId));
  }

  public Record getRecordFromNode(long recordId, boolean replicate) {
    if (replicate) {
      synchronizeReplicas(recordId, syncReplicaCount);
      startReadRepair(recordId);
    }

    return readData(recordId);
  }

  @Override
  public RecordMetadata getRecordMetadataFromNode(long id) {
    final Record record = readData(id);
    if (record == null)
      return null;

    return new RecordMetadata(record.getId(), record.getVersion());
  }

  public void updateRecord(long recordId, Record data) {
    waitTillJoin();
    int retryCount = 0;

    while (true) {
      retryCount++;

      final long successorId = findSuccessor(recordId);

      if (successorId != recordId) {
        if (!remoteNodeUpdate(recordId, data, retryCount, successorId))
          continue;

        return;
      }

      updateRecordInNode(recordId, data);

      return;
    }
  }

  public void deleteRecord(long recordId, ODHTRecordVersion version) {
    waitTillJoin();
    int retryCount = 0;

    while (true) {
      retryCount++;

      final long successorId = findSuccessor(recordId);

      if (successorId != nodeId) {
        if (!remoteNodeDelete(recordId, version, retryCount, successorId))
          continue;

        return;
      }

      deleteRecordFromNode(recordId, version);
      return;
    }
  }

  public long[] findMissedRecords(long[] ids, ODHTRecordVersion[] versions) {
    ArrayList<Long> result = new ArrayList<Long>();

    for (int i = 0; i < ids.length; i++) {
      final Record record = db.get(ids[i]);
      if (record == null)
        result.add(ids[i]);
      else if (versions[i].compareTo(record.getVersion()) > 0)
        result.add(ids[i]);
    }

    long[] missedRecords = new long[result.size()];
    for (int i = 0; i < missedRecords.length; i++)
      missedRecords[i] = result.get(i);

    return missedRecords;
  }

  public void updateReplica(final Record replica, final boolean async) {
    putReplica(replica.getId(), replica);
  }

  @Override
  public RecordMetadata[] getNodeRecordsForInterval(long startId, long endId) {
    List<RecordMetadata> recordMetadatas = new ArrayList<RecordMetadata>();
    int processedRecords = 0;

    for (Record record : db.subMap(startId, true, endId, false).values()) {
      recordMetadatas.add(new RecordMetadata(record.getId(), record.getVersion()));
      processedRecords++;
      if (processedRecords >= MAX_RECORDS_TO_RETURN)
        break;
    }

    RecordMetadata[] result = new RecordMetadata[recordMetadatas.size()];
    result = recordMetadatas.toArray(result);

    return result;
  }

  private RemoteNodeCallResult<Record> remoteNodeCreate(long id, String data, int retryCount, long nodeId) {
    Logger logger = LoggerFactory.getLogger(OLocalDHTNode.class);

    final ODHTNode node = nodeLookup.findById(nodeId);

    if (node == null) {
      if (retryCount < MAX_RETRIES) {
        logger.debug("Node {} is offline, retry {}-d time.", nodeId, retryCount);
        return new RemoteNodeCallResult<Record>(true, null);
      } else {
        logger.error("Node {} is offline, retry limit is reached.", nodeId);
        throw new ONodeOfflineException("Node " + nodeId + " is offline, retry limit is reached.", null, nodeId);
      }
    }

    try {
      return new RemoteNodeCallResult<Record>(false, node.createRecordInNode(id, data));
    } catch (ONodeOfflineException ooe) {
      if (retryCount < MAX_RETRIES) {
        logger.debug("Node {} is offline, retry {}-d time.", nodeId, retryCount);
        return new RemoteNodeCallResult<Record>(true, null);
      } else {
        logger.error("Node " + nodeId + " is offline, retry limit is reached.");
        throw new ONodeOfflineException("Node " + nodeId + " is offline, retry limit is reached.", null, nodeId);
      }
    }
  }

  private boolean remoteNodeUpdate(long id, Record data, int retryCount, long nodeId) {
    Logger logger = LoggerFactory.getLogger(OLocalDHTNode.class);

    final ODHTNode node = nodeLookup.findById(nodeId);

    if (node == null) {
      if (retryCount < MAX_RETRIES) {
        logger.debug("Node {} is offline, retry {}-d time.", nodeId, retryCount);
        return false;
      } else {
        logger.error("Node " + nodeId + " is offline, retry limit is reached.");
        throw new ONodeOfflineException("Node " + nodeId + " is offline, retry limit is reached.", null, nodeId);
      }
    }

    try {
      node.updateRecordInNode(id, data);
    } catch (ONodeOfflineException ooe) {
      if (retryCount < MAX_RETRIES) {
        logger.debug("Node {} is offline, retry {}-d time.", nodeId, retryCount);
        return false;
      } else {
        logger.error("Node " + nodeId + " is offline, retry limit is reached.");
        throw new ONodeOfflineException("Node " + nodeId + " is offline, retry limit is reached.", null, nodeId);
      }
    }

    return true;
  }

  private boolean remoteNodeDelete(long id, ODHTRecordVersion version, int retryCount, long nodeId) {
    Logger logger = LoggerFactory.getLogger(OLocalDHTNode.class);

    final ODHTNode node = nodeLookup.findById(nodeId);

    if (node == null) {
      if (retryCount < MAX_RETRIES) {
        logger.debug("Node {} is offline, retry {}-d time.", nodeId, retryCount);
        return false;
      } else {
        logger.error("Node " + nodeId + " is offline, retry limit is reached.");
        throw new ONodeOfflineException("Node " + nodeId + " is offline, retry limit is reached.", null, nodeId);
      }
    }

    try {
      node.deleteRecordFromNode(id, version);
    } catch (ONodeOfflineException ooe) {
      if (retryCount < MAX_RETRIES) {
        logger.debug("Node {} is offline, retry {}-d time.", nodeId, retryCount);
        return false;
      } else {
        logger.error("Node " + nodeId + " is offline, retry limit is reached.");
        throw new ONodeOfflineException("Node " + nodeId + " is offline, retry limit is reached.", null, nodeId);
      }
    }

    return true;
  }

  private Record addData(long id, String data) {
    lockManager.acquireLock(Thread.currentThread(), id, OLockManager.LOCK.EXCLUSIVE);
    try {
      return this.merkleTree.addData(id, data);
    } finally {
      lockManager.releaseLock(Thread.currentThread(), id, OLockManager.LOCK.EXCLUSIVE);
    }
  }

  private void updateData(long id, Record record) {
    lockManager.acquireLock(Thread.currentThread(), id, OLockManager.LOCK.EXCLUSIVE);
    try {
      this.merkleTree.updateData(id, record.getVersion(), record.getData());
    } finally {
      lockManager.releaseLock(Thread.currentThread(), id, OLockManager.LOCK.EXCLUSIVE);
    }
  }

  private RemoteNodeCallResult<Record> remoteNodeGet(long key, long nodeId, int retryCount) {
    Logger logger = LoggerFactory.getLogger(OLocalDHTNode.class);

    ODHTNode node = nodeLookup.findById(nodeId);
    if (node == null) {
      if (retryCount < MAX_RETRIES) {
        logger.debug("Node {} is offline, retry {}-d time.", nodeId, retryCount);
        return new RemoteNodeCallResult<Record>(true, null);
      } else {
        logger.error("Node " + nodeId + " is offline, retry limit is reached.");
        throw new ONodeOfflineException("Node " + nodeId + " is offline, retry limit is reached.", null, nodeId);
      }
    }

    try {
      return new RemoteNodeCallResult<Record>(false, node.getRecordFromNode(key, true));
    } catch (ONodeOfflineException e) {
      if (retryCount < MAX_RETRIES) {
        logger.debug("Node {} is offline, retry {}-d time.", nodeId, retryCount);
        return new RemoteNodeCallResult<Record>(true, null);
      } else {
        logger.error("Node " + nodeId + " is offline, retry limit is reached.");
        throw e;
      }
    }
  }

  private void waitTillJoin() {
    Logger logger = LoggerFactory.getLogger(OLocalDHTNode.class);

    while (!NodeState.PRODUCTION.equals(state)) {
      logger.info("Wait till node {} will be joined.", nodeId);

      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException(e);
      }
    }
  }

  private Record readData(long dataId) {
    Record data;
    lockManager.acquireLock(Thread.currentThread(), dataId, OLockManager.LOCK.SHARED);
    try {
      data = db.get(dataId);
    } finally {
      lockManager.releaseLock(Thread.currentThread(), dataId, OLockManager.LOCK.SHARED);
    }
    return data;
  }

  private void putReplica(long id, Record replica) {
    lockManager.acquireLock(Thread.currentThread(), id, OLockManager.LOCK.SHARED);
    try {
      this.merkleTree.updateReplica(id, replica);
    } finally {
      lockManager.releaseLock(Thread.currentThread(), id, OLockManager.LOCK.SHARED);
    }
  }

  private void removeData(long id, ODHTRecordVersion version) {
    lockManager.acquireLock(Thread.currentThread(), id, OLockManager.LOCK.EXCLUSIVE);
    try {
      merkleTree.deleteData(id, version);
    } finally {
      lockManager.releaseLock(Thread.currentThread(), id, OLockManager.LOCK.EXCLUSIVE);
    }
  }

  private void cleanOutData(long id, ODHTRecordVersion version) {
    lockManager.acquireLock(Thread.currentThread(), id, OLockManager.LOCK.EXCLUSIVE);
    try {
      merkleTree.deleteData(id, version, false);
    } finally {
      lockManager.releaseLock(Thread.currentThread(), id, OLockManager.LOCK.EXCLUSIVE);
    }
  }

  public int size() {
    int count = 0;

    for (Record record : db.values()) {
      if (!record.isTombstone())
        count++;
    }

    return count;
  }

  public NodeState state() {
    return state;
  }

  public void stabilize() {
    Logger logger = LoggerFactory.getLogger(this.getClass().getName() + ".stabilize");

    boolean result = false;

    int retryCount = 0;
    sCycle: while (!result) {
      final long successorId = fingerPoints.get(0);

      ODHTNode successor = nodeLookup.findById(successorId);
      if (successor == null) {
        handleSuccessorOfflineCase(retryCount, successorId);

        retryCount++;
        result = false;
        continue;
      }

      final Long predecessor;

      try {
        predecessor = successor.getPredecessor();
      } catch (ONodeOfflineException ooe) {
        handleSuccessorOfflineCase(retryCount, successorId);

        retryCount++;
        result = false;
        continue;
      }

      if (predecessor > -1 && insideInterval(this.nodeId, successorId, predecessor, false)) {
        logger.debug("Successor {} is going to be changed to {}", successorId, predecessor);

        result = fingerPoints.compareAndSet(0, successorId, predecessor);

        if (result)
          logger.debug("Successor was successfully changed");
        else
          logger.debug("Successor change was failed");

        if (result) {
          successor = nodeLookup.findById(predecessor);
          if (successor == null) {
            handleSuccessorOfflineCase(retryCount, predecessor);

            retryCount++;
            result = false;
            continue;
          }
        }

        drawRing();
      } else
        result = true;

      long prevPredecessor = -1;
      try {
        if (successor.getNodeId() != nodeId)
          prevPredecessor = successor.notifyParent(nodeId);
      } catch (ONodeOfflineException ooe) {
        handleSuccessorOfflineCase(retryCount, successor.getNodeId());

        retryCount++;
        result = false;
        continue;
      }

      if (prevPredecessor > -1) {
        ODHTNode prevPredecessorNode = nodeLookup.findById(prevPredecessor);
        if (prevPredecessorNode != null)
          try {
            prevPredecessorNode.requestStabilization();
          } catch (ONodeOfflineException e) {
            // ignore
          }
      }

      try {
        updateSuccessors(successor);
      } catch (ONodeOfflineException e) {
        handleSuccessorOfflineCase(retryCount, successor.getNodeId());

        retryCount++;
        result = false;
      }
    }
  }

  private void updateSuccessors(ODHTNode successor) {
    final long successorId = successor.getNodeId();
    sListCycle: while (true) {
      final int successorsSize = Math.max(((int) Math.ceil(Math.log(nodeLookup.size()) / Math.log(2))), replicaCount);

      final List<Long> processedSuccessors = new ArrayList<Long>();
      ODHTNode dhtNode = successor;

      while (processedSuccessors.size() < successorsSize && dhtNode.getNodeId() != nodeId)
        try {
          final long nodeSuccessor = dhtNode.getSuccessor();
          dhtNode = nodeLookup.findById(nodeSuccessor);

          if (dhtNode == null)
            continue;

          processedSuccessors.add(nodeSuccessor);
        } catch (ONodeOfflineException e) {
          if (dhtNode.getNodeId() != successorId)
            continue sListCycle;
        }

      long[] newSuccessors = new long[processedSuccessors.size()];

      for (int i = 0; i < newSuccessors.length; i++)
        newSuccessors[i] = processedSuccessors.get(i);

      successorsList = newSuccessors;
      break;
    }
  }

  private void replicateRecord(Record record) {
    if (replicaCount < 1)
      return;

    Logger logger = LoggerFactory.getLogger(this.getClass().getName() + ".replicateRecord");
    logger.debug("Replication of record {} replica count is {}", record.getId(), replicaCount);

    long[] replicaHolders = getSuccessors();

    logger.debug("Replica holders for record {} are {}", record.getId(), replicaHolders);

    if (replicaHolders == null || replicaHolders.length == 0)
      return;

    final Set<Long> asyncReplicas = chooseReplicas(replicaHolders, replicaCount - syncReplicaCount);

    logger.debug("Async replica holders for record {} are {}", record.getId(), asyncReplicas);

    for (long replicaHolderId : replicaHolders) {
      final ODHTNode replicaHolderNode = nodeLookup.findById(replicaHolderId);
      if (replicaHolderNode == null) {
        logger.error("Replica holder with id " + replicaHolderId + " is absent.");
        continue;
      }

      try {
        boolean async = asyncReplicas.contains(replicaHolderId);

        logger.debug("Replication of record {} with async flag is set to {} to holder {}", new Object[] { record.getId(), async,
            replicaHolderId });

        replicaHolderNode.updateReplica(record, async);

        logger.debug("Replication of record {} with async flag is set to {} to holder {}  was finished.",
            new Object[] { record.getId(), async, replicaHolderId });

      } catch (Exception e) {
        logger.error("Exception during replication of record " + record.getId() + " to node " + replicaHolderId);
        // ignore
      }
    }

    logger.debug("Replication of record {} was finished.", record.getId());
  }

  private Set<Long> chooseReplicas(long[] replicaHolders, int replicaCount) {
    replicaCount = Math.min(replicaCount, replicaHolders.length);

    final Set<Long> replicas = new HashSet<Long>();

    int holderIndex = 0;
    while (replicas.size() < replicaCount) {
      replicas.add(replicaHolders[holderIndex]);
      holderIndex++;
    }

    return replicas;
  }

  private void replicateRecord(long id) {
    final Record replica = readData(id);
    replicateRecord(replica);
  }

  private void handleSuccessorOfflineCase(int retryCount, long successorId) {
    Logger logger = LoggerFactory.getLogger(OLocalDHTNode.class);

    if (retryCount < MAX_RETRIES) {
      logger.debug("Successor {} is offline will try to find new one and retry. {}-d retry.", successorId, retryCount);

      final long newSuccessorId = findSuccessor(nodeId);
      if (fingerPoints.compareAndSet(0, successorId, newSuccessorId)) {
        final ODHTNode newSuccessorNode = nodeLookup.findById(newSuccessorId);
        if (newSuccessorNode != null)
          try {
            newSuccessorNode.notifyParent(nodeId);
          } catch (ONodeOfflineException noe) {
            fingerPoints.compareAndSet(0, newSuccessorId, successorId);
          }
      }
    } else {
      logger.error("Successor " + successorId + " is offline will try to find new one and retry." + " Max retry count is reached.");
      throw new ONodeOfflineException("Successor " + successorId + " is offline will try to find new one and retry."
          + " Max retry count is reached.", null, successorId);
    }
  }

  public void fixFingers() {
    int nextValue = next.intValue();

    fingerPoints.set(nextValue, findSuccessor((nodeId + 1 << nextValue) & Long.MAX_VALUE));

    next.compareAndSet(nextValue, nextValue + 1);

    while (next.intValue() > 62) {
      nextValue = next.intValue();
      if (nextValue > 62)
        next.compareAndSet(nextValue, 1);
    }
  }

  public void fixPredecessor() {
    boolean result = false;

    while (!result) {
      long predecessorId = predecessor.longValue();

      if (predecessorId > -1 && nodeLookup.findById(predecessorId) == null) {
        result = predecessor.compareAndSet(predecessorId, -1);
      } else
        result = true;
    }
  }

  public long notifyParent(long nodeId) {
    boolean result = false;
    long prevPredecessor = -1;

    Logger logger = LoggerFactory.getLogger(this.getClass().getName() + ".notifyParent");

    while (!result) {
      long predecessorId = predecessor.longValue();

      if (predecessorId < 0 || (insideInterval(predecessorId, this.nodeId, nodeId, false))) {
        prevPredecessor = predecessorId;

        result = predecessor.compareAndSet(predecessorId, nodeId);
        if (result)
          logger.info("New predecessor is {}", nodeId);
        else {
          logger.info("Predecessor setup was failed.");
          prevPredecessor = -1;
        }

        if (result && predecessorId < 0 && state == NodeState.JOIN) {
          state = NodeState.PRODUCTION;
          logger.info("Status was changed to {}", state);
        }

        drawRing();
      } else
        result = true;
    }

    // log("Parent check is finished.");

    return prevPredecessor;
  }

  public void requestStabilization() {
    stabilize();
  }

  private static boolean insideInterval(long from, long to, long value, boolean rightIsIncluded) {
    if (to > from) {
      if (rightIsIncluded)
        return from < value && to >= value;
      else
        return from < value && to > value;
    } else {
      if (rightIsIncluded)
        return !(value > to && value <= from);
      else
        return !(value >= to && value <= from);
    }
  }

  private void drawRing() {
    Logger logger = LoggerFactory.getLogger(OLocalDHTNode.class);
    try {
      StringBuilder builder = new StringBuilder();

      builder.append("Ring : ");

      builder.append(nodeId);
      ODHTNode node = this;

      Set<Long> processedIds = new HashSet<Long>();
      processedIds.add(nodeId);

      long successor = node.getSuccessor();
      while (!processedIds.contains(successor)) {
        builder.append("-").append(successor);
        processedIds.add(successor);

        node = nodeLookup.findById(successor);
        if (node == null)
          return;

        successor = node.getSuccessor();
      }

      builder.append(".");

      logger.info(builder.toString());
    } catch (ONodeOfflineException noe) {
      // ignore
    }
  }

  private void compareNodes(ODetachedMerkleTreeNode localNode, ODetachedMerkleTreeNode remoteNode, long remoteNodeId) {
    final long localPredecessor = predecessor.get();

    if (localPredecessor < 0)
      throw new NodeSynchronizationFailedException("Node predecessor is absent.");

    final Logger logger = LoggerFactory.getLogger(LocalMaintenanceProtocol.class);

    logger.debug("Comparing nodes local: {} and remote: {}", localNode, remoteNode);

    if (remoteNode.isLeaf()) {
      logger.debug("Comparing nodes remote node {} is a leaf.", remoteNode);

      for (int i = 0; i < remoteNode.getRecordsCount(); i++) {
        final RecordMetadata recordMetadata = remoteNode.getRecordMetadata(i);
        if (insideInterval(localPredecessor, nodeId, recordMetadata.getId(), true)) {
          final Record dbRecord = db.get(recordMetadata.getId());
          if (dbRecord == null || dbRecord.getVersion().compareTo(recordMetadata.getVersion()) < 0)
            missingRecord(recordMetadata.getId(), remoteNodeId);
        }
      }
    } else if (localNode.isLeaf()) {
      logger.debug("Comparing nodes local node {} is leaf.", localNode);

      long startId = localNode.getStartId();

      final long endId = localNode.getEndId();

      RecordMetadata[] nodeMetadatas = getNodeRecordsForInterval(startId, endId);

      while (nodeMetadatas.length > 0) {
        for (RecordMetadata nodeMetadata : nodeMetadatas) {
          final Record dbRecord = db.get(nodeMetadata.getId());

          if (dbRecord == null || dbRecord.getVersion().compareTo(nodeMetadata.getVersion()) < 0)
            missingRecord(nodeMetadata.getId(), remoteNodeId);
        }

        startId = nodeMetadatas[nodeMetadatas.length - 1].getId() + 1;

        nodeMetadatas = getNodeRecordsForInterval(startId, endId);
      }
    }

  }

  @Override
  public ODetachedMerkleTreeNode findMerkleTreeNode(ODetachedMerkleTreeNode remoteNode, long requestorId) {
    final ODetachedMerkleTreeNode localNode = merkleTree.getEquivalentNode(remoteNode);
    if (localNode == null)
      return null;

    compareNodes(localNode, remoteNode, requestorId);

    return localNode;
  }

  private void missingRecord(long recordId, long remoteNodeId) {
    final ODHTNode remoteNode = nodeLookup.findById(remoteNodeId);
    if (remoteNode == null)
      throw new NodeSynchronizationFailedException("Node with id " + remoteNodeId + " is absent in ring.");

    final Logger logger = LoggerFactory.getLogger(GlobalMaintenanceProtocol.class);
    logger.debug("Record with id {} is missed for current node {} will get it from node {}", new Object[] { recordId, nodeId,
        remoteNodeId });

    final Record replica = remoteNode.getRecordFromNode(recordId, false);
    logger.debug("Replica with id {} was found for node {} with content {}", new Object[] { recordId, nodeId, replica });

    if (replica != null) {
      putReplica(recordId, replica);

      logger.debug("Replica with id {} was updated for node {}", new Object[] { recordId, nodeId });
    }
  }

  private final class GlobalMaintenanceProtocol implements Callable<Void> {
    private final Logger logger = LoggerFactory.getLogger(GlobalMaintenanceProtocol.class);

    public Void call() throws Exception {
      long idToTest = nodeId;

      gmCycle: while (!Thread.currentThread().isInterrupted()) {
        try {
          if (state == null || !state.equals(NodeState.PRODUCTION)) {
            logger.debug("Illegal state , wait till node will be ready to serve requests in DHT ring.");

            continue;
          }

          logger.debug("Finding record with id next to {}", idToTest);

          Long nextId = nextInDB(idToTest);
          if (nextId == null) {
            logger.debug("There are no records with id next to {}", idToTest);
            continue;
          }

          logger.debug("Record with id {} is closest successor for id {}", nextId, idToTest);

          logger.debug("Finding successor for record {}", nextId);
          long successor = findSuccessor(nextId);

          logger.debug("Successor for record is {}", successor);
          if (nodeId == successor) {
            idToTest = nodeId;

            logger.debug("We are owner of {} record. So we start from the beginning", nextId);
            continue;
          }

          final ODHTNode successorNode = nodeLookup.findById(successor);
          if (successorNode == null) {
            idToTest = nodeId;

            logger.debug("Successor with id {} is absent, starting from beginning", successor);
            continue;
          }

          logger.debug("Find the successors for node {}", successor);
          long[] successors = successorNode.getSuccessors();
          logger.debug("Successors list for node {} is {}", successor, successors);

          for (long s : successors) {
            if (s == nodeId) {
              idToTest = nodeId;

              logger.debug("We are owner of {} record. So we start from the beginning", nextId);

              continue gmCycle;
            }
          }

          List<Long> nodesToReplicate = new ArrayList<Long>();
          nodesToReplicate.add(successor);
          for (long s : successors)
            nodesToReplicate.add(s);

          logger.debug("List of nodes to replicate records starting from {} to {} is {}", new Object[] { nextId, successor,
              nodesToReplicate });

          final Iterator<RecordMetadata> iterator = new ODHTRingIterator(db, idToTest, successor);

          List<Long> ids = new ArrayList<Long>(64);
          List<ODHTRecordVersion> versions = new ArrayList<ODHTRecordVersion>(64);

          while (iterator.hasNext()) {
            final RecordMetadata recordMetadata = iterator.next();

            ids.add(recordMetadata.getId());
            versions.add(recordMetadata.getVersion());

            if (ids.size() >= 64)
              cleanOutForeignRecords(ids, versions, nodesToReplicate);
          }

          if (!ids.isEmpty())
            cleanOutForeignRecords(ids, versions, nodesToReplicate);

          idToTest = successor;
        } catch (Exception e) {
          logger.error(e.toString(), e);

          idToTest = nodeId;
        } finally {
          try {
            Thread.sleep(1000);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        }
      }

      return null;
    }

    private void cleanOutForeignRecords(List<Long> ids, List<ODHTRecordVersion> versions, List<Long> nodesToReplicate) {
      final ODHTRecordVersion[] arrayVersions = new ODHTRecordVersion[versions.size()];
      final long[] arrayIDs = new long[ids.size()];

      for (int i = 0; i < arrayIDs.length; i++)
        arrayIDs[i] = ids.get(i);

      versions.toArray(arrayVersions);

      for (long replicaHolderId : nodesToReplicate) {
        final ODHTNode node = nodeLookup.findById(replicaHolderId);

        if (node == null) {
          logger.debug("Node with id {} is absent. Continue replication with other node.", replicaHolderId);
          continue;
        }

        try {
          logger.debug("Finding missing ids for records with ids {} and versions {}", arrayIDs, arrayVersions);

          final long[] missingIds = node.findMissedRecords(arrayIDs, arrayVersions);

          logger.debug("Missing ids are {}", missingIds);

          for (long missingId : missingIds) {
            final Record replica = db.get(missingId);
            if (replica != null)
              node.updateReplica(replica, false);
          }

        } catch (ONodeOfflineException noe) {
          logger.debug("Node with id {} is absent. Continue replication with other node.", replicaHolderId);
        }
      }

      logger.debug("Clean out foreign records");

      for (int i = 0; i < ids.size(); i++) {
        final long recordId = ids.get(i);
        final ODHTRecordVersion version = versions.get(i);

        logger.debug("Cleaning out of record with id {} and version {}", recordId, version);
        try {
          cleanOutData(recordId, version);
          logger.debug("Record with id {} was cleaned out.", recordId);
        } catch (OConcurrentModificationException e) {
          logger.debug("Record with id {} and version {}" + " is out of date and can not be cleaned out", recordId, version);
        }
      }

      ids.clear();
      versions.clear();

      logger.debug("Clean out was completed.");
    }

    private Long nextInDB(long id) {
      Long result = db.higherKey(id);

      if (result != null)
        return result;

      if (id > 0)
        result = db.ceilingKey(0L);

      if (result != null && result != id)
        return result;

      return null;
    }
  }

  private static final class GlobalMaintenanceProtocolThreadFactory implements ThreadFactory {
    private static final AtomicInteger counter = new AtomicInteger();

    private final long                 nodeId;

    private GlobalMaintenanceProtocolThreadFactory(long nodeId) {
      this.nodeId = nodeId;
    }

    public Thread newThread(Runnable r) {
      final Thread thread = new Thread(r);

      thread.setName("Global Maintenance Protocol for node '" + nodeId + "' [" + counter.incrementAndGet() + "]");
      thread.setDaemon(true);

      return thread;
    }
  }

  private static final class LocalMaintenanceProtocolThreadFactory implements ThreadFactory {
    private static final AtomicInteger counter = new AtomicInteger();

    private final long                 nodeId;

    private LocalMaintenanceProtocolThreadFactory(long nodeId) {
      this.nodeId = nodeId;
    }

    public Thread newThread(Runnable r) {
      final Thread thread = new Thread(r);

      thread.setName("Local Maintenance Protocol for node '" + nodeId + "' [" + counter.incrementAndGet() + "]");
      thread.setDaemon(true);

      return thread;
    }
  }

  private final class LocalMaintenanceProtocol implements Callable<Void> {
    private final Logger logger = LoggerFactory.getLogger(LocalMaintenanceProtocol.class);

    @Override
    public Void call() throws Exception {
      lmCycle: while (!Thread.currentThread().isInterrupted()) {
        try {
          if (state == null || !state.equals(NodeState.PRODUCTION)) {
            logger.debug("Node is not in production, wait till status will be changed.");

            continue;
          }

          final long localPredecessor = predecessor.get();
          logger.debug("Predecessor of node is {}", localPredecessor);

          if (localPredecessor == -1) {
            logger.debug("Predecessor for node {} is absent. Start from the beginning.", nodeId);
            continue;
          }

          logger.debug("Retrieving successors for node {}", nodeId);
          final long[] replicaHolderIDs = getSuccessors();
          logger.debug("Replica holders for node {} are {}", nodeId, replicaHolderIDs);

          for (long replicaHolderID : replicaHolderIDs) {
            final List<ODetachedMerkleTreeNode> roots = merkleTree.getRootNodesForInterval(localPredecessor + 1, nodeId);
            logger.debug("Merkle roots for node {} for interval from {} to {} are {}", new Object[] { replicaHolderID,
                localPredecessor + 1, nodeId, roots });

            for (final ODetachedMerkleTreeNode rootNode : roots)
              synchronizeNode(rootNode, replicaHolderID);
          }
        } catch (Exception e) {
          logger.error(e.toString(), e);
        } finally {
          try {
            Thread.sleep(100);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        }
      }

      return null;
    }

    private void synchronizeNode(final ODetachedMerkleTreeNode localTreeNode, final long remoteNodeId) {
      if (localTreeNode == null)
        throw new NodeSynchronizationFailedException("Passed Local Merkle Tree node is null.");

      final ODHTNode remoteNode = nodeLookup.findById(remoteNodeId);

      if (remoteNode == null)
        throw new NodeSynchronizationFailedException("Node with id " + remoteNodeId + " is absent.");

      final ODetachedMerkleTreeNode remoteTreeNode = remoteNode.findMerkleTreeNode(localTreeNode, nodeId);

      if (remoteTreeNode == null)
        throw new NodeSynchronizationFailedException("Related remote Merkle tree node is null.");

      compareNodes(localTreeNode, remoteTreeNode, remoteNodeId);

      final long localPredecessor = predecessor.get();
      if (localPredecessor < 0)
        throw new NodeSynchronizationFailedException("Node with id " + remoteNodeId + " is absent.");

      if (!localTreeNode.isLeaf() && !remoteTreeNode.isLeaf()) {
        logger.debug("Comparing children for Merkle tree node {} ", localTreeNode);

        for (int i = 0; i < 64; i++) {

          logger.debug("Compare {}-th children", i);

          ODetachedMerkleTreeNode childNode = merkleTree.getChildNode(localTreeNode, i);
          final long startNodeId = childNode.getStartId();
          final long endNodeId = childNode.getEndId();

          if (insideInterval(localPredecessor, nodeId, startNodeId, true)
              || insideInterval(localPredecessor, nodeId, endNodeId, true)) {
            if (!Arrays.equals(childNode.getHash(), remoteTreeNode.getChildHash(i)))
              logger.debug("Children of {}-th node are not equal.");
            synchronizeNode(localTreeNode, remoteNodeId);
          }
        }
      }
    }
  }

  private static final class ReadRepairThreadFactory implements ThreadFactory {
    private static final AtomicInteger counter = new AtomicInteger();

    private final long                 nodeId;

    private ReadRepairThreadFactory(long nodeId) {
      this.nodeId = nodeId;
    }

    public Thread newThread(Runnable r) {
      final Thread thread = new Thread(r);

      thread.setName("Read Repair Protocol for node '" + nodeId + "' [" + counter.incrementAndGet() + "]");
      thread.setDaemon(true);

      return thread;
    }
  }

  private final class ReadRepairTask implements Callable<Void> {
    private final long recordId;

    private ReadRepairTask(long recordId) {
      this.recordId = recordId;
    }

    @Override
    public Void call() throws Exception {
      try {
        if (!NodeState.PRODUCTION.equals(state))
          return null;

        synchronizeReplicas(recordId, replicaCount);
      } catch (Throwable t) {

      }

      return null;
    }
  }

  private static final class NodeSynchronizationFailedException extends RuntimeException {
    private NodeSynchronizationFailedException(String message) {
      super(message);
    }

    private NodeSynchronizationFailedException(String message, Throwable cause) {
      super(message, cause);
    }
  }

  private static final class RemoteNodeCallResult<T> {
    private boolean repeat;
    private T       data;

    private RemoteNodeCallResult(boolean repeat, T data) {
      this.repeat = repeat;
      this.data = data;
    }
  }
}
