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
import java.util.concurrent.ScheduledExecutorService;
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

  private int                                nextIndex             = 0;
  private final Object                       fingersLock           = new Object();

  private final OLockManager<Long, Runnable> lockManager           = new OLockManager<Long, Runnable>(true, 500);
  private volatile long[]                    successorsList        = new long[0];

  private final ScheduledExecutorService     gmExecutorService;
  private final ScheduledExecutorService     lmExecutorService;
  private final ExecutorService              readRepairService;

  private volatile NodeState                 state;

  private final OMerkleTree                  merkleTree            = new OMerkleTree(db);

  private final int                          replicaCount;
  private final int                          syncReplicaCount;

  private final boolean                      useReadRepair;
  private final boolean                      useAntiEntropy;
  private final boolean                      useGlobalMaintainence;

  public OLocalDHTNode(long nodeId, int replicaCount, int syncReplicaCount, boolean useReadRepair, boolean useAntiEntropy,
      boolean useGlobalMaintainence) {
    this.useReadRepair = useReadRepair;
    this.useAntiEntropy = useAntiEntropy;
    this.useGlobalMaintainence = useGlobalMaintainence;

    this.nodeId = nodeId;
    for (int i = 0; i < fingerPoints.length(); i++)
      fingerPoints.set(i, -1);

    this.replicaCount = replicaCount;
    this.syncReplicaCount = syncReplicaCount;

    gmExecutorService = Executors.newSingleThreadScheduledExecutor(new GlobalMaintenanceProtocolThreadFactory(nodeId));
    lmExecutorService = Executors.newSingleThreadScheduledExecutor(new LocalMaintenanceProtocolThreadFactory(nodeId));

    readRepairService = new ThreadPoolExecutor(0, Runtime.getRuntime().availableProcessors() / 2, 60L, TimeUnit.SECONDS,
        new ArrayBlockingQueue<Runnable>(256), new ReadRepairThreadFactory(nodeId), new ThreadPoolExecutor.CallerRunsPolicy());
  }

  public NavigableMap<Long, Record> getDb() {
    return db;
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

    if (useGlobalMaintainence)
      gmExecutorService.scheduleWithFixedDelay(new GlobalMaintenanceProtocol(), 100, 100, TimeUnit.MILLISECONDS);

    if (useAntiEntropy)
      lmExecutorService.scheduleWithFixedDelay(new LocalMaintenanceProtocol(), 1, 1, TimeUnit.SECONDS);

    random.setSeed((new Random()).nextLong());

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
        if (useGlobalMaintainence)
          gmExecutorService.scheduleWithFixedDelay(new GlobalMaintenanceProtocol(), 100, 100, TimeUnit.MILLISECONDS);
        if (useAntiEntropy)
          lmExecutorService.scheduleWithFixedDelay(new LocalMaintenanceProtocol(), 1, 1, TimeUnit.SECONDS);
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

    int retryCount = 0;
    while (true) {
      logger.debug("Successor request for key {}", key);
      final long successorId = fingerPoints.get(0);

      ODHTRingInterval ringInterval = new ODHTRingInterval(ODHTRingInterval.increment(nodeId), successorId);

      if (ringInterval.insideInterval(key)) {
        if (!isNodeOffline(successorId)) {
          logger.debug("Key {} inside interval {} - {} ", new Object[] { key, nodeId, successorId });
          return successorId;
        } else {
          final long[] successors = successorsList;

          // we are alone return itself.
          if (nodeLookup.size() == 0)
            return nodeId;

          for (final long nthSuccessorId : successors) {
            ringInterval = new ODHTRingInterval(ODHTRingInterval.increment(nodeId), nthSuccessorId);
            if (ringInterval.insideInterval(key)) {
              if (!isNodeOffline(nthSuccessorId)) {
                logger.debug("Key {} inside interval {} - {} ", new Object[] { key, nodeId, nthSuccessorId });
                return nthSuccessorId;
              }
            }
          }
        }
      }

      retryCount++;
      if (retryCount > MAX_RETRIES) {
        throw new ONodeOfflineException("Successor can not be found, id to process " + key + ", successor " + getSuccessor()
            + ", auxiliary successors " + Arrays.toString(successorsList) + ", finger points " + fingerPoints, null, successorId);
      }

      long precedingNodeId = findClosestPrecedingFinger(key);

      logger.debug("Closest preceding node for key {} is {}", key, precedingNodeId);

      if (precedingNodeId == nodeId) {
        logger.error("Successor was changed, retry");
        continue;
      }

      ODHTNode precedingNode = nodeLookup.findById(precedingNodeId);
      if (precedingNode == null) {
        if (precedingNodeId != getSuccessor())
          clearFingerPoint(precedingNodeId);
        else
          requestStabilization();

        continue;
      }

      try {
        final long successorResult = precedingNode.findSuccessor(key);
        logger.debug("Successor for key {} is {}", key, successorResult);
        return successorResult;
      } catch (ONodeOfflineException e) {
        if (precedingNodeId != getSuccessor())
          clearFingerPoint(precedingNodeId);
        else
          requestStabilization();
      }
    }
  }

  private boolean isNodeOffline(long nodeId) {
    return nodeLookup.findById(nodeId) == null;
  }

  private void clearFingerPoint(long nodeId) {
    synchronized (fingersLock) {
      fingerPointCycle: while (true) {
        for (int i = 1; i < fingerPoints.length(); i++) {
          final long finger = fingerPoints.get(i);
          if (finger > -1 && nodeId == finger)
            if (!fingerPoints.compareAndSet(i, finger, -1))
              continue fingerPointCycle;
        }
        return;
      }
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
    final ODHTRingInterval ringInterval = new ODHTRingInterval(ODHTRingInterval.increment(nodeId), ODHTRingInterval.decrement(key));
    for (int i = fingerPoints.length() - 1; i >= 0; i--) {
      final long fingerPoint = fingerPoints.get(i);
      if (fingerPoint > -1 && ringInterval.insideInterval(fingerPoint)) {
        return fingerPoint;
      }
    }

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
      if (retryCount > 5)
        System.out.println("Test");

      final long successorId = findSuccessor(recordId);

      logger.debug("Successor for record {} is {}", recordId, successorId);
      retryCount++;

      if (successorId == nodeId) {
        return createRecordInNode(recordId, data);
      } else {
        final RemoteNodeCallResult<Record> result = remoteNodeCreate(recordId, data, retryCount, successorId);
        if (result.repeat) {
          logger.error("Successor {} for record {} is offline, retry.", successorId, recordId);
          requestStabilization();
          continue;
        }

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

        if (remoteNodeGetResult.repeat) {
          logger.error("Successor {} for record {} is offline, retry.", successorId, recordId);

          requestStabilization();
          continue;
        }

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
              } else if (cp > 0) {
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
          logger.error("Exception during synchronization of record " + recordId + " for node " + holderId, e);
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
            logger
                .error("Exception during replication of record with id " + recordId + " for node " + replicaHolder.getNodeId(), e);
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
  public Record[] getRecordsFromNode(long[] ids, boolean replicate) {
    final ArrayList<Record> records = new ArrayList<Record>();
    for (long id : ids) {
      final Record record = getRecordFromNode(id, replicate);
      if (record != null)
        records.add(record);
    }

    Record[] recordsArray = new Record[records.size()];
    recordsArray = records.toArray(recordsArray);

    return recordsArray;
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

    Logger logger = LoggerFactory.getLogger(this.getClass().getName() + ".updateRecord");
    while (true) {
      retryCount++;

      final long successorId = findSuccessor(recordId);

      if (successorId != recordId) {
        if (!remoteNodeUpdate(recordId, data, retryCount, successorId)) {
          logger.error("Successor {} for record {} is offline, retry.", successorId, recordId);
          requestStabilization();
          continue;
        }

        return;
      }

      updateRecordInNode(recordId, data);

      return;
    }
  }

  public void deleteRecord(long recordId, ODHTRecordVersion version) {
    waitTillJoin();
    int retryCount = 0;

    Logger logger = LoggerFactory.getLogger(this.getClass().getName() + ".deleteRecord");
    while (true) {
      retryCount++;

      final long successorId = findSuccessor(recordId);

      if (successorId != nodeId) {
        if (!remoteNodeDelete(recordId, version, retryCount, successorId)) {
          logger.error("Successor {} for record {} is offline, retry.", successorId, recordId);
          requestStabilization();
          continue;
        }

        return;
      }

      deleteRecordFromNode(recordId, version);
      return;
    }
  }

  public long[] findMissedRecords(RecordMetadata[] recordMetadatas) {
    ArrayList<Long> result = new ArrayList<Long>();

    for (RecordMetadata recordMetadata : recordMetadatas) {
      final Record record = db.get(recordMetadata.getId());

      if (record == null)
        result.add(recordMetadata.getId());

      else if (recordMetadata.getVersion().compareTo(record.getVersion()) > 0)
        result.add(recordMetadata.getId());
    }

    long[] missedRecords = new long[result.size()];
    for (int i = 0; i < missedRecords.length; i++)
      missedRecords[i] = result.get(i);

    return missedRecords;
  }

  @Override
  public void updateReplicas(Record[] replicas, boolean async) {
    for (Record replica : replicas)
      updateReplica(replica, async);
  }

  public void updateReplica(final Record replica, final boolean async) {
    putReplica(replica.getId(), replica);
  }

  @Override
  public RecordMetadata[] getRecordsForIntervalFromNode(long startId, long endId) {
    final List<RecordMetadata> recordMetadatas = new ArrayList<RecordMetadata>();

    int processedRecords = 0;

    final ODHTRingIterator ringIterator = new ODHTRingIterator(db, startId, endId);
    while (ringIterator.hasNext()) {
      final RecordMetadata recordMetadata = ringIterator.next();
      if (recordMetadata != null)
        recordMetadatas.add(recordMetadata);

      processedRecords++;

      if (processedRecords >= MAX_RECORDS_TO_RETURN)
        break;
    }

    RecordMetadata[] result = new RecordMetadata[recordMetadatas.size()];
    result = recordMetadatas.toArray(result);

    return result;
  }

  public void shutdown() throws Exception {
    gmExecutorService.shutdownNow();
    lmExecutorService.shutdownNow();
    readRepairService.shutdownNow();

    if (!gmExecutorService.awaitTermination(180000, TimeUnit.MILLISECONDS))
      throw new IllegalStateException("GM service was not terminated.");

    if (!lmExecutorService.awaitTermination(180000, TimeUnit.MILLISECONDS))
      throw new IllegalStateException("LM service was not terminated.");

    if (!readRepairService.awaitTermination(180000, TimeUnit.MILLISECONDS))
      throw new IllegalStateException("RR service was not terminated.");
  }

  private RemoteNodeCallResult<Record> remoteNodeCreate(long id, String data, int retryCount, long remoteNodeId) {
    Logger logger = LoggerFactory.getLogger(OLocalDHTNode.class);

    final ODHTNode node = nodeLookup.findById(remoteNodeId);

    if (node == null) {
      if (retryCount < MAX_RETRIES) {
        logger.debug("Node {} is offline, retry {}-d time.", remoteNodeId, retryCount);

        try {
          Thread.sleep(50);
        } catch (InterruptedException e) {
          logger.error("Thread execution was interrupted.", e);
          throw new ORemoteNodeCallException("Thread execution was interrupted.", e, remoteNodeId);
        }

        return new RemoteNodeCallResult<Record>(true, null);
      } else {
        logger.error("Node {} is offline, retry limit is reached.", remoteNodeId);
        throw new ONodeOfflineException("Node " + remoteNodeId + " is offline, retry limit is reached.", null, remoteNodeId);
      }
    }

    try {
      return new RemoteNodeCallResult<Record>(false, node.createRecordInNode(id, data));
    } catch (ONodeOfflineException ooe) {
      if (retryCount < MAX_RETRIES) {
        logger.debug("Node {} is offline, retry {}-d time.", remoteNodeId, retryCount);

        try {
          Thread.sleep(50);
        } catch (InterruptedException e) {
          logger.error("Thread execution was interrupted.", e);
          throw new ORemoteNodeCallException("Thread execution was interrupted.", e, remoteNodeId);
        }

        return new RemoteNodeCallResult<Record>(true, null);
      } else {
        logger.error("Node " + remoteNodeId + " is offline, retry limit is reached.");
        throw new ONodeOfflineException("Node " + remoteNodeId + " is offline, retry limit is reached.", null, remoteNodeId);
      }
    }
  }

  private boolean remoteNodeUpdate(long id, Record data, int retryCount, long remoteNodeId) {
    Logger logger = LoggerFactory.getLogger(OLocalDHTNode.class);

    final ODHTNode node = nodeLookup.findById(remoteNodeId);

    if (node == null) {
      if (retryCount < MAX_RETRIES) {
        logger.debug("Node {} is offline, retry {}-d time.", remoteNodeId, retryCount);

        try {
          Thread.sleep(50);
        } catch (InterruptedException e) {
          logger.error("Thread execution was interrupted.", e);
          throw new ORemoteNodeCallException("Thread execution was interrupted.", e, remoteNodeId);
        }

        return false;
      } else {
        logger.error("Node " + remoteNodeId + " is offline, retry limit is reached.");
        throw new ONodeOfflineException("Node " + remoteNodeId + " is offline, retry limit is reached.", null, remoteNodeId);
      }
    }

    try {
      node.updateRecordInNode(id, data);
    } catch (ONodeOfflineException ooe) {
      if (retryCount < MAX_RETRIES) {
        logger.debug("Node {} is offline, retry {}-d time.", remoteNodeId, retryCount);

        try {
          Thread.sleep(50);
        } catch (InterruptedException e) {
          logger.error("Thread execution was interrupted.", e);
          throw new ORemoteNodeCallException("Thread execution was interrupted.", e, remoteNodeId);
        }

        return false;
      } else {
        logger.error("Node " + remoteNodeId + " is offline, retry limit is reached.");
        throw new ONodeOfflineException("Node " + remoteNodeId + " is offline, retry limit is reached.", null, remoteNodeId);
      }
    }

    return true;
  }

  private boolean remoteNodeDelete(long id, ODHTRecordVersion version, int retryCount, long remoteNodeId) {
    Logger logger = LoggerFactory.getLogger(OLocalDHTNode.class);

    final ODHTNode node = nodeLookup.findById(remoteNodeId);

    if (node == null) {
      if (retryCount < MAX_RETRIES) {
        logger.debug("Node {} is offline, retry {}-d time.", remoteNodeId, retryCount);

        try {
          Thread.sleep(50);
        } catch (InterruptedException e) {
          logger.error("Thread execution was interrupted.", e);
          throw new ORemoteNodeCallException("Thread execution was interrupted.", e, remoteNodeId);
        }

        return false;
      } else {
        logger.error("Node " + remoteNodeId + " is offline, retry limit is reached.");
        throw new ONodeOfflineException("Node " + remoteNodeId + " is offline, retry limit is reached.", null, remoteNodeId);
      }
    }

    try {
      node.deleteRecordFromNode(id, version);
    } catch (ONodeOfflineException ooe) {
      if (retryCount < MAX_RETRIES) {
        logger.debug("Node {} is offline, retry {}-d time.", remoteNodeId, retryCount);

        try {
          Thread.sleep(50);
        } catch (InterruptedException e) {
          logger.error("Thread execution was interrupted.", e);
          throw new ORemoteNodeCallException("Thread execution was interrupted.", e, remoteNodeId);
        }

        return false;
      } else {
        logger.error("Node " + remoteNodeId + " is offline, retry limit is reached.");
        throw new ONodeOfflineException("Node " + remoteNodeId + " is offline, retry limit is reached.", null, remoteNodeId);
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

  private RemoteNodeCallResult<Record> remoteNodeGet(long key, long remoteNodeId, int retryCount) {
    Logger logger = LoggerFactory.getLogger(OLocalDHTNode.class);

    ODHTNode node = nodeLookup.findById(remoteNodeId);
    if (node == null) {
      if (retryCount < MAX_RETRIES) {
        logger.debug("Node {} is offline, retry {}-d time.", remoteNodeId, retryCount);

        try {
          Thread.sleep(50);
        } catch (InterruptedException e) {
          logger.error("Thread execution was interrupted.", e);
          throw new ORemoteNodeCallException("Thread execution was interrupted.", e, remoteNodeId);
        }

        return new RemoteNodeCallResult<Record>(true, null);
      } else {
        logger.error("Node " + remoteNodeId + " is offline, retry limit is reached.");
        throw new ONodeOfflineException("Node " + remoteNodeId + " is offline, retry limit is reached.", null, remoteNodeId);
      }
    }

    try {
      return new RemoteNodeCallResult<Record>(false, node.getRecordFromNode(key, true));
    } catch (ONodeOfflineException e) {
      if (retryCount < MAX_RETRIES) {
        logger.debug("Node {} is offline, retry {}-d time.", remoteNodeId, retryCount);

        try {
          Thread.sleep(50);
        } catch (InterruptedException ie) {
          logger.error("Thread execution was interrupted.", ie);
          throw new ORemoteNodeCallException("Thread execution was interrupted.", ie, remoteNodeId);
        }

        return new RemoteNodeCallResult<Record>(true, null);
      } else {
        logger.error("Node " + remoteNodeId + " is offline, retry limit is reached.");
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
    final Logger logger = LoggerFactory.getLogger(this.getClass().getName() + ".stabilize");

    try {
      boolean result = false;

      int retryCount = 0;
      while (!result) {
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

        final ODHTRingInterval ringInterval = new ODHTRingInterval(ODHTRingInterval.increment(this.nodeId),
            ODHTRingInterval.decrement(successorId));

        if (predecessor > -1 && ringInterval.insideInterval(predecessor)) {
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
    } catch (Exception e) {
      logger.error("Error during node stabilization.", e);
    }
  }

  private void updateSuccessors(ODHTNode successor) {
    final long successorId = successor.getNodeId();
    sListCycle: while (true) {
      final int successorsSize = Math.max(((int) Math.ceil(Math.log(nodeLookup.size()) / Math.log(2))), replicaCount);

      final List<Long> processedSuccessors = new ArrayList<Long>();
      ODHTNode dhtNode = successor;

      while (processedSuccessors.size() < successorsSize)
        try {
          final long nodeSuccessor = dhtNode.findSuccessor(ODHTRingInterval.increment(dhtNode.getNodeId()));
          if (nodeSuccessor == nodeId)
            break;

          dhtNode = nodeLookup.findById(nodeSuccessor);

          if (dhtNode == null)
            continue sListCycle;

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

    int processedReplicaHolders = 0;
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
        logger.error("Exception during replication of record " + record.getId() + " to node " + replicaHolderId, e);
        // ignore
      }
      processedReplicaHolders++;
      if (processedReplicaHolders >= replicaCount)
        break;
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

  private void handleSuccessorOfflineCase(int retryCount, long successorId) throws InterruptedException {
    Logger logger = LoggerFactory.getLogger(OLocalDHTNode.class);

    if (retryCount < MAX_RETRIES) {
      logger.debug("Successor {} is offline will try to find new one and retry. {}-d retry.", successorId, retryCount);

      Thread.sleep(50);

      final long newSuccessorId = findSuccessor(ODHTRingInterval.increment(nodeId));
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
      logger.error("Successor " + successorId + " for node " + nodeId + " is offline. Max retry count is reached.");
      throw new ONodeOfflineException("Successor " + successorId + " is offline for node " + nodeId
          + "  will try to find new one and retry." + " Max retry count is reached.", null, successorId);
    }
  }

  public void fixFingers() {
    synchronized (fingersLock) {
      final Logger logger = LoggerFactory.getLogger(this.getClass().getName() + ".fixPredecessor");
      try {
        int prevIndex = nextIndex - 1;
        long prevFinger = -1;

        while (prevFinger == -1 && prevIndex >= 0) {
          prevFinger = fingerPoints.get(prevIndex);
          prevIndex--;
        }

        final long fingerPointCandidate = findSuccessor((nodeId + 1 << nextIndex) & Long.MAX_VALUE);

        if (prevFinger > -1) {
          final ODHTRingInterval allowedInterval = new ODHTRingInterval(ODHTRingInterval.increment(prevFinger),
              ODHTRingInterval.decrement(nodeId));
          if (!allowedInterval.insideInterval(fingerPointCandidate)) {
            nextIndex = 1;
            return;
          }

        }

        fingerPoints.set(nextIndex, fingerPointCandidate);

        nextIndex++;

        if (nextIndex > 62)
          nextIndex = 1;
      } catch (Exception e) {
        logger.error("Error during fingers fix", e);
      }
    }
  }

  public void fixPredecessor() {
    final Logger logger = LoggerFactory.getLogger(this.getClass().getName() + ".fixPredecessor");
    try {
      boolean result = false;

      while (!result) {
        long predecessorId = predecessor.get();

        if (predecessorId > -1 && nodeLookup.findById(predecessorId) == null) {
          result = predecessor.compareAndSet(predecessorId, -1);
        } else
          result = true;
      }
    } catch (Exception e) {
      logger.error("Exception during predecessor fix", e);
    }
  }

  public long notifyParent(long predecessorCandidateId) {
    boolean result = false;
    long prevPredecessor = -1;

    Logger logger = LoggerFactory.getLogger(this.getClass().getName() + ".notifyParent");

    while (!result) {
      long predecessorId = predecessor.longValue();

      final ODHTRingInterval ringInterval = new ODHTRingInterval(ODHTRingInterval.increment(predecessorId),
          ODHTRingInterval.decrement(this.nodeId));

      if (predecessorId < 0 || ringInterval.insideInterval(predecessorCandidateId)) {
        prevPredecessor = predecessorId;

        result = predecessor.compareAndSet(predecessorId, predecessorCandidateId);
        if (result)
          logger.info("New predecessor is {}", predecessorCandidateId);
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

  @Override
  public ODetachedMerkleTreeNode findMerkleTreeNode(ODetachedMerkleTreeNode remoteNode) {
    final ODetachedMerkleTreeNode localNode = merkleTree.getEquivalentNode(remoteNode);
    if (localNode == null)
      return null;

    return localNode;
  }

  private final class GlobalMaintenanceProtocol implements Runnable {
    private final Logger logger = LoggerFactory.getLogger(GlobalMaintenanceProtocol.class);

    private long         idToTest;

    private GlobalMaintenanceProtocol() {
      idToTest = nodeId;
    }

    public void run() {
      try {
        if (state == null || !state.equals(NodeState.PRODUCTION)) {
          logger.debug("Illegal state , wait till node will be ready to serve requests in DHT ring.");

          return;
        }

        logger.debug("Finding record with id next to {}", idToTest);

        Long nextId = nextInDB(idToTest);
        if (nextId == null) {
          logger.debug("There are no records with id next to {}", idToTest);
          return;
        }

        logger.debug("Record with id {} is closest successor for id {}", nextId, idToTest);

        logger.debug("Finding successor for record {}", nextId);
        long successor = findSuccessor(nextId);

        logger.debug("Successor for record is {}", successor);
        if (nodeId == successor) {
          idToTest = nodeId;

          logger.debug("We are owner of {} record. So we start from the beginning", nextId);
          return;
        }

        final ODHTNode successorNode = nodeLookup.findById(successor);
        if (successorNode == null) {
          idToTest = nodeId;

          logger.debug("Successor with id {} is absent, starting from beginning", successor);
          return;
        }

        logger.debug("Find the successors for node {}", successor);

        long[] successors = successorNode.getSuccessors();
        if (successors.length > replicaCount) {
          final long[] oldSuccessors = successors;
          successors = new long[replicaCount];

          System.arraycopy(oldSuccessors, 0, successors, 0, successors.length);
        }

        logger.debug("Successors list for node {} is {}", successor, successors);

        for (long s : successors) {
          if (s == nodeId) {
            idToTest = nodeId;

            logger.debug("We are owner of {} record. So we start from the beginning", nextId);

            return;
          }
        }

        List<Long> nodesToReplicate = new ArrayList<Long>();
        nodesToReplicate.add(successor);
        for (long s : successors)
          nodesToReplicate.add(s);

        logger.debug("List of nodes to replicate records starting from {} to {} is {}", new Object[] { nextId, successor,
            nodesToReplicate });

        final Iterator<RecordMetadata> iterator = new ODHTRingIterator(db, idToTest, successor);

        final List<RecordMetadata> recordMetadatas = new ArrayList<RecordMetadata>(64);

        while (iterator.hasNext()) {
          final RecordMetadata recordMetadata = iterator.next();

          recordMetadatas.add(recordMetadata);

          if (recordMetadatas.size() >= 64)
            cleanOutForeignRecords(recordMetadatas, nodesToReplicate);
        }

        if (!recordMetadatas.isEmpty())
          cleanOutForeignRecords(recordMetadatas, nodesToReplicate);

        idToTest = successor;
      } catch (Exception e) {
        logger.error(e.toString(), e);

        idToTest = nodeId;
      }
    }

    private void cleanOutForeignRecords(List<RecordMetadata> recordMetadatas, List<Long> nodesToReplicate) {
      RecordMetadata[] metadatas = new RecordMetadata[recordMetadatas.size()];
      metadatas = recordMetadatas.toArray(metadatas);

      for (long replicaHolderId : nodesToReplicate) {
        final ODHTNode node = nodeLookup.findById(replicaHolderId);

        if (node == null) {
          logger.debug("Node with id {} is absent. Continue replication with other node.", replicaHolderId);
          continue;
        }

        try {
          logger.debug("Finding missing ids for records with metadata {}", recordMetadatas);

          final long[] missingIds = node.findMissedRecords(metadatas);

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

      for (RecordMetadata recordMetadata : metadatas) {
        logger.debug("Cleaning out of record with id {} and version {}", recordMetadata.getId(), recordMetadata.getVersion());
        try {
          cleanOutData(recordMetadata.getId(), recordMetadata.getVersion());
          logger.debug("Record with id {} was cleaned out.", recordMetadata.getId());
        } catch (OConcurrentModificationException e) {
          logger.debug("Record with id {} and version {} is out of date and can not be cleaned out", recordMetadata.getId(),
              recordMetadata.getVersion());
        }
      }

      recordMetadatas.clear();

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

  private final class LocalMaintenanceProtocol implements Runnable {
    private final Logger logger = LoggerFactory.getLogger(LocalMaintenanceProtocol.class);

    @Override
    public void run() {
      try {
        if (state == null || !state.equals(NodeState.PRODUCTION)) {
          logger.debug("Node is not in production, wait till status will be changed.");
          return;
        }

        final long localPredecessor = predecessor.get();
        logger.debug("Predecessor of node is {}", localPredecessor);

        if (localPredecessor == -1) {
          logger.debug("Predecessor for node {} is absent. Start from the beginning.", nodeId);
          return;
        }

        logger.debug("Retrieving successors for node {}", nodeId);
        long[] replicaHolderIDs = getSuccessors();

        if (replicaHolderIDs.length > replicaCount) {
          final long[] oldReplicaHolderIDs = replicaHolderIDs;
          replicaHolderIDs = new long[replicaCount];

          System.arraycopy(oldReplicaHolderIDs, 0, replicaHolderIDs, 0, replicaHolderIDs.length);
        }

        logger.debug("Replica holders for node {} are {}", nodeId, replicaHolderIDs);

        for (long replicaHolderID : replicaHolderIDs) {
          try {
            final long startId = ODHTRingInterval.increment(localPredecessor);
            final long endId = nodeId;

            final List<ODetachedMerkleTreeNode> roots = getRootsForInterval(merkleTree, startId, endId);
            for (final ODetachedMerkleTreeNode rootNode : roots)
              synchronizeNode(rootNode, replicaHolderID);
          } catch (Exception e) {
            logger.error("Error during replication of content to node " + replicaHolderID, e);
          }
        }
      } catch (Exception e) {
        logger.error(e.toString(), e);
      }
    }

    private List<ODetachedMerkleTreeNode> getRootsForInterval(final OMerkleTree tree, final long startId, final long endId) {
      if (startId <= endId)
        return tree.getRootNodesForInterval(startId, endId);

      final List<ODetachedMerkleTreeNode> result = new ArrayList<ODetachedMerkleTreeNode>();

      final List<ODetachedMerkleTreeNode> firstInterval = tree.getRootNodesForInterval(startId, Long.MAX_VALUE);
      final List<ODetachedMerkleTreeNode> secondInterval = tree.getRootNodesForInterval(0, endId);

      result.addAll(firstInterval);

      if (!secondInterval.isEmpty() && !firstInterval.isEmpty()
          && Arrays.equals(secondInterval.get(0).getHash(), firstInterval.get(firstInterval.size() - 1).getHash())) {
        if (secondInterval.size() > 1)
          result.addAll(secondInterval.subList(1, secondInterval.size()));
      } else {
        result.addAll(secondInterval);
      }

      return result;
    }

    private void synchronizeNode(final ODetachedMerkleTreeNode localTreeNode, final long remoteNodeId) {
      if (localTreeNode == null)
        throw new NodeSynchronizationFailedException("Passed Local Merkle Tree node is null.");

      final ODHTNode remoteNode = nodeLookup.findById(remoteNodeId);

      if (remoteNode == null)
        throw new NodeSynchronizationFailedException("Node with id " + remoteNodeId + " is absent.");

      final ODetachedMerkleTreeNode remoteTreeNode = remoteNode.findMerkleTreeNode(localTreeNode);

      if (remoteTreeNode == null)
        throw new NodeSynchronizationFailedException("Related remote Merkle tree node is null.");

      compareNodes(localTreeNode, remoteTreeNode, remoteNodeId);

      final long localPredecessor = predecessor.get();
      if (localPredecessor < 0)
        throw new NodeSynchronizationFailedException("Predecessor " + localPredecessor + " is absent.");

      final ODHTRingInterval nodeInterval = new ODHTRingInterval(ODHTRingInterval.increment(localPredecessor), nodeId);

      if (!localTreeNode.isLeaf() && !remoteTreeNode.isLeaf()) {
        for (int i = 0; i < 64; i++) {

          ODetachedMerkleTreeNode childNode = merkleTree.getChildNode(localTreeNode, i);
          final long startNodeId = childNode.getStartId();
          final long endNodeId = childNode.getEndId();

          final ODHTRingInterval treeNodeInterval = new ODHTRingInterval(startNodeId, endNodeId);

          if (nodeInterval.intersection(treeNodeInterval) != null) {
            if (!Arrays.equals(childNode.getHash(), remoteTreeNode.getChildHash(i)))
              synchronizeNode(childNode, remoteNodeId);
          }
        }
      }
    }

    private void compareNodes(ODetachedMerkleTreeNode localNode, ODetachedMerkleTreeNode remoteNode, long remoteNodeId) {
      if (Arrays.equals(remoteNode.getHash(), localNode.getHash()))
        return;

      compareWithLocal(localNode, remoteNode, remoteNodeId);
      compareWithRemote(localNode, remoteNode, remoteNodeId);
    }

    private void compareWithRemote(ODetachedMerkleTreeNode localNode, ODetachedMerkleTreeNode remoteNode, long remoteNodeId) {
      final long localPredecessor = predecessor.get();

      if (localPredecessor < 0)
        throw new NodeSynchronizationFailedException("Node predecessor is absent.");

      final ODHTNode remoteDHTNode = nodeLookup.findById(remoteNodeId);
      if (remoteDHTNode == null)
        throw new NodeSynchronizationFailedException("Remote node with id " + remoteNodeId + " is offline.");

      final ODHTRingInterval dhtNodeInterval = new ODHTRingInterval(ODHTRingInterval.increment(localPredecessor), nodeId);
      final ODHTRingInterval treeNodeInterval = new ODHTRingInterval(localNode.getStartId(), localNode.getEndId());

      final ODHTRingInterval recordsInterval = dhtNodeInterval.intersection(treeNodeInterval);
      if (recordsInterval == null)
        return;

      long startId = recordsInterval.getStart();
      long endId = recordsInterval.getEnd();

      final ArrayList<Record> recordsToReplicate = new ArrayList<Record>();
      if (localNode.isLeaf()) {
        RecordMetadata[] nodeMetadatas = getRecordsForIntervalFromNode(startId, endId);

        while (nodeMetadatas.length > 0) {
          final long[] missedIds = remoteDHTNode.findMissedRecords(nodeMetadatas);
          for (long missedId : missedIds) {
            final Record record = db.get(missedId);
            if (record != null)
              recordsToReplicate.add(record);

            if (recordsToReplicate.size() >= 64) {
              sendRecords(recordsToReplicate, remoteNodeId);
              recordsToReplicate.clear();
            }

          }

          startId = ODHTRingInterval.increment(nodeMetadatas[nodeMetadatas.length - 1].getId());

          if (recordsInterval.insideInterval(startId))
            nodeMetadatas = getRecordsForIntervalFromNode(startId, endId);
        }

        if (!recordsToReplicate.isEmpty())
          sendRecords(recordsToReplicate, remoteNodeId);
      } else if (remoteNode.isLeaf()) {
        final Set<RecordMetadata> merkleTreeMetadataSet = new HashSet<RecordMetadata>();

        RecordMetadata[] recordMetadatas = getRecordsForIntervalFromNode(startId, endId);

        for (int i = 0; i < remoteNode.getRecordsCount(); i++)
          merkleTreeMetadataSet.add(remoteNode.getRecordMetadata(i));

        while (recordMetadatas.length > 0) {
          for (RecordMetadata recordMetadata : recordMetadatas) {
            if (!merkleTreeMetadataSet.contains(recordMetadata)) {
              final Record record = db.get(recordMetadata.getId());
              if (record != null)
                recordsToReplicate.add(record);
            }

            if (recordsToReplicate.size() >= 64) {
              sendRecords(recordsToReplicate, remoteNodeId);
              recordsToReplicate.clear();
            }
          }
          startId = ODHTRingInterval.increment(recordMetadatas[recordMetadatas.length - 1].getId());

          if (recordsInterval.insideInterval(startId))
            recordMetadatas = getRecordsForIntervalFromNode(startId, endId);
        }

        if (!recordsToReplicate.isEmpty())
          sendRecords(recordsToReplicate, remoteNodeId);

      }
    }

    private void compareWithLocal(ODetachedMerkleTreeNode localNode, ODetachedMerkleTreeNode remoteNode, long remoteNodeId) {
      final long localPredecessor = predecessor.get();

      if (localPredecessor < 0)
        throw new NodeSynchronizationFailedException("Node predecessor is absent.");

      final ODHTRingInterval dhtNodeInterval = new ODHTRingInterval(ODHTRingInterval.increment(localPredecessor), nodeId);

      final ArrayList<Long> recordsToFetch = new ArrayList<Long>();

      if (remoteNode.isLeaf()) {
        for (int i = 0; i < remoteNode.getRecordsCount(); i++) {
          final RecordMetadata recordMetadata = remoteNode.getRecordMetadata(i);
          if (dhtNodeInterval.insideInterval(recordMetadata.getId())) {
            final Record dbRecord = db.get(recordMetadata.getId());
            if (dbRecord == null || dbRecord.getVersion().compareTo(recordMetadata.getVersion()) < 0)
              recordsToFetch.add(recordMetadata.getId());
          }

          if (recordsToFetch.size() >= 64) {
            fetchRecords(recordsToFetch, remoteNodeId);
            recordsToFetch.clear();
          }
        }

        if (!recordsToFetch.isEmpty())
          fetchRecords(recordsToFetch, remoteNodeId);

      } else if (localNode.isLeaf()) {
        final ODHTRingInterval treeNodeInterval = new ODHTRingInterval(localNode.getStartId(), localNode.getEndId());

        final ODHTRingInterval recordsInterval = dhtNodeInterval.intersection(treeNodeInterval);
        if (recordsInterval == null)
          return;

        long startId = recordsInterval.getStart();
        long endId = recordsInterval.getEnd();

        final ODHTNode remoteDHTNode = nodeLookup.findById(remoteNodeId);
        if (remoteDHTNode == null)
          throw new NodeSynchronizationFailedException("Remote node with id " + remoteNodeId + " is offline.");

        RecordMetadata[] nodeMetadatas = remoteDHTNode.getRecordsForIntervalFromNode(startId, endId);

        while (nodeMetadatas.length > 0) {
          for (RecordMetadata nodeMetadata : nodeMetadatas) {
            final Record dbRecord = db.get(nodeMetadata.getId());

            if (dbRecord == null || dbRecord.getVersion().compareTo(nodeMetadata.getVersion()) < 0)
              recordsToFetch.add(nodeMetadata.getId());

            if (recordsToFetch.size() >= 64) {
              fetchRecords(recordsToFetch, remoteNodeId);
              recordsToFetch.clear();
            }
          }

          startId = ODHTRingInterval.increment(nodeMetadatas[nodeMetadatas.length - 1].getId());

          if (recordsInterval.insideInterval(startId))
            nodeMetadatas = remoteDHTNode.getRecordsForIntervalFromNode(startId, endId);
        }

        if (!recordsToFetch.isEmpty())
          fetchRecords(recordsToFetch, remoteNodeId);
      }
    }

    private void fetchRecords(List<Long> missedRecords, long remoteNodeId) {
      final ODHTNode remoteNode = nodeLookup.findById(remoteNodeId);
      if (remoteNode == null)
        throw new NodeSynchronizationFailedException("Node with id " + remoteNodeId + " is absent in ring.");

      final Logger logger = LoggerFactory.getLogger(LocalMaintenanceProtocol.class);
      logger.debug("Records with ids {} are missed in current node {} will get it from node {}", new Object[] { missedRecords,
          nodeId, remoteNodeId });

      final long[] missedRecordsArray = new long[missedRecords.size()];
      for (int i = 0; i < missedRecordsArray.length; i++)
        missedRecordsArray[i] = missedRecords.get(i);

      final Record[] replicas = remoteNode.getRecordsFromNode(missedRecordsArray, false);
      logger.debug("Replicas  {} were found for node {}", new Object[] { replicas, nodeId });

      for (Record replica : replicas) {
        putReplica(replica.getId(), replica);
        logger.debug("Replica with id {} was updated for node {}", new Object[] { replica.getId(), nodeId });
      }
    }

    private void sendRecords(List<Record> missedRecords, long remoteNodeId) {
      final ODHTNode remoteNode = nodeLookup.findById(remoteNodeId);
      if (remoteNode == null)
        throw new NodeSynchronizationFailedException("Node with id " + remoteNodeId + " is absent in ring.");

      final Logger logger = LoggerFactory.getLogger(LocalMaintenanceProtocol.class);
      logger.debug("Records  {} are missed in node {} will get it from current node {}", new Object[] { missedRecords,
          remoteNodeId, nodeId });

      Record[] missedRecordsArray = new Record[missedRecords.size()];
      missedRecordsArray = missedRecords.toArray(missedRecordsArray);

      remoteNode.updateReplicas(missedRecordsArray, false);

      logger.debug("Replicas {} were updated for node {}", new Object[] { missedRecords, remoteNodeId });
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
    private final Logger logger = LoggerFactory.getLogger(ReadRepairTask.class);
    private final long   recordId;

    private ReadRepairTask(long recordId) {
      this.recordId = recordId;
    }

    @Override
    public Void call() throws Exception {
      try {
        if (!useReadRepair)
          return null;

        if (!NodeState.PRODUCTION.equals(state))
          return null;

        synchronizeReplicas(recordId, replicaCount);
      } catch (Exception e) {
        logger.error("Exception during read repair for record " + recordId, e);
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
