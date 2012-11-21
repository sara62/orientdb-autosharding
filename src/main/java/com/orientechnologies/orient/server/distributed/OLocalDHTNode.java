package com.orientechnologies.orient.server.distributed;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
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
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceArray;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.orientechnologies.common.concur.lock.OLockManager;
import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.id.OClusterPositionNodeId;
import com.orientechnologies.orient.core.id.ONodeId;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.storage.ORecordDuplicatedException;

/**
 * @author Andrey Lomakin
 * @since 17.08.12
 */
public final class OLocalDHTNode implements ODHTNode {
  private static final int                         MAX_RETRIES           = 10;
  private static final int                         MAX_RECORDS_TO_RETURN = 64;

  private final ONodeAddress                       nodeAddress;

  private final AtomicReferenceArray<ONodeAddress> fingerPoints          = new AtomicReferenceArray<ONodeAddress>(
                                                                             ONodeId.NODE_SIZE_BITS - 1);
  private AtomicReference<ONodeAddress>            predecessor           = new AtomicReference<ONodeAddress>();

  private final NavigableMap<ORecordId, Record>    db                    = new ConcurrentSkipListMap<ORecordId, Record>();

  private volatile ODHTNodeLookup                  nodeLookup;

  private int                                      nextIndex             = 0;
  private final Object                             fingersLock           = new Object();

  private final OLockManager<ORecordId, Runnable>  lockManager           = new OLockManager<ORecordId, Runnable>(true, 500);

  private volatile ONodeAddress[]                  successorsList        = new ONodeAddress[0];

  private final ScheduledExecutorService           gmExecutorService;
  private final ScheduledExecutorService           lmExecutorService;
  private final ExecutorService                    readRepairService;

  private volatile NodeState                       state;

  private final OMerkleTree                        merkleTree            = new OMerkleTree(db, 1);

  private final int                                replicaCount;
  private final int                                syncReplicaCount;

  private final boolean                            useReadRepair;
  private final boolean                            useAntiEntropy;
  private final boolean                            useGlobalMaintainence;

  public OLocalDHTNode(ONodeAddress nodeAddress, int replicaCount, int syncReplicaCount, boolean useReadRepair,
      boolean useAntiEntropy, boolean useGlobalMaintainence) {
    this.useReadRepair = useReadRepair;
    this.useAntiEntropy = useAntiEntropy;
    this.useGlobalMaintainence = useGlobalMaintainence;

    this.nodeAddress = nodeAddress;

    this.replicaCount = replicaCount;
    this.syncReplicaCount = syncReplicaCount;

    gmExecutorService = Executors.newSingleThreadScheduledExecutor(new GlobalMaintenanceProtocolThreadFactory(nodeAddress));
    lmExecutorService = Executors.newSingleThreadScheduledExecutor(new LocalMaintenanceProtocolThreadFactory(nodeAddress));

    readRepairService = new ThreadPoolExecutor(0, Runtime.getRuntime().availableProcessors() / 2, 60L, TimeUnit.SECONDS,
        new ArrayBlockingQueue<Runnable>(256), new ReadRepairThreadFactory(nodeAddress), new ThreadPoolExecutor.CallerRunsPolicy());
  }

  public NavigableMap<ORecordId, Record> getDb() {
    return db;
  }

  public void setNodeLookup(ODHTNodeLookup nodeLookup) {
    this.nodeLookup = nodeLookup;
  }

  public void createDHT() {
    Logger logger = LoggerFactory.getLogger(OLocalDHTNode.class);
    logger.info("New ring creation was started");

    fingerPoints.set(0, nodeAddress);
    state = NodeState.PRODUCTION;

    if (useGlobalMaintainence)
      gmExecutorService.scheduleWithFixedDelay(new GlobalMaintenanceProtocol(), 100, 100, TimeUnit.MILLISECONDS);

    if (useAntiEntropy)
      lmExecutorService.scheduleWithFixedDelay(new LocalMaintenanceProtocol(), 1, 1, TimeUnit.SECONDS);

    logger.info("New ring was created");
  }

  public ONodeAddress getNodeAddress() {
    return nodeAddress;
  }

  public boolean joinDHT(ONodeAddress nodeAddress) {
    Logger logger = LoggerFactory.getLogger(OLocalDHTNode.class);

    try {
      logger.info("Join is started using node with address {}", nodeAddress);

      final ODHTNode node = nodeLookup.findById(nodeAddress);
      if (node == null) {
        logger.error("Node {} is absent.", nodeAddress);
        return false;
      }

      if (state == null) {
        if (useGlobalMaintainence)
          gmExecutorService.scheduleWithFixedDelay(new GlobalMaintenanceProtocol(), 100, 100, TimeUnit.MILLISECONDS);
        if (useAntiEntropy)
          lmExecutorService.scheduleWithFixedDelay(new LocalMaintenanceProtocol(), 1, 1, TimeUnit.SECONDS);
      }

      state = NodeState.JOIN;

      int retryCount = 0;
      while (true) {
        try {
          logger.info("Wait till node {} will be ready to process requests.", nodeAddress);
          while (node.state() == null)
            Thread.sleep(100);
          logger.info("Node {} is ready to process requests", nodeAddress);

          final ONodeAddress successorAddress = node.findSuccessor(this.nodeAddress.getNodeId());
          fingerPoints.set(0, successorAddress);

          ODHTNode successor = nodeLookup.findById(successorAddress);
          if (successor == null) {
            if (retryCount < MAX_RETRIES) {
              logger.debug("Node {} is offline, retry {}-d time.", successorAddress, retryCount);
              retryCount++;
              Thread.sleep(100);
            } else {
              logger.error("Node " + successorAddress + " is offline, max retries is reached");
              return false;
            }

            continue;
          }

          final ONodeAddress prevPredecessor = successor.notifyParent(this.nodeAddress);
          if (prevPredecessor != null) {
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
          if (ooe.getNodeId().equals(nodeAddress.getNodeId())) {
            logger.error("Node with address " + nodeAddress + " is absent.");
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
      return false;
    }
  }

  public ONodeAddress findSuccessor(ONodeId id) {
    Logger logger = LoggerFactory.getLogger(this.getClass().getName() + ".findSuccessor");

    int retryCount = 0;
    while (true) {
      logger.debug("Successor request for id {}", id);
      final ONodeAddress successorAddress = fingerPoints.get(0);

      ODHTRingInterval ringInterval = new ODHTRingInterval(nodeAddress.getNodeId().add(ONodeId.ONE), successorAddress.getNodeId());

      if (ringInterval.insideInterval(id)) {
        if (!isNodeOffline(successorAddress)) {
          logger
              .debug("Id {} inside interval {} - {} ", new Object[]{id, nodeAddress.getNodeId(), successorAddress.getNodeId()});
          return successorAddress;
        } else {
          final ONodeAddress[] successors = successorsList;

          // we are alone return itself.
          if (successors.length == 0)
            return nodeAddress;

          for (final ONodeAddress nthSuccessorAddress : successors) {
            ringInterval = new ODHTRingInterval(nodeAddress.getNodeId().add(ONodeId.ONE), nthSuccessorAddress.getNodeId());
            if (ringInterval.insideInterval(id)) {
              if (!isNodeOffline(nthSuccessorAddress)) {
                logger.debug("Id {} inside interval {} - {} ",
                    new Object[] { id, nodeAddress.getNodeId(), nthSuccessorAddress.getNodeId() });
                return nthSuccessorAddress;
              }
            }
          }
        }
      }

      retryCount++;
      if (retryCount > MAX_RETRIES) {
        throw new ONodeOfflineException("Successor can not be found using node " + nodeAddress + " , id to process " + id
            + ", successor " + getSuccessor() + ", auxiliary successors " + Arrays.toString(successorsList) + ", finger points "
            + fingerPoints, null, successorAddress.getNodeId());
      }

      ONodeAddress precedingNodeAddress = findClosestPrecedingFinger(id);

      logger.debug("Closest preceding node for id {} is {}", id, precedingNodeAddress);

      if (precedingNodeAddress.equals(nodeAddress)) {
        logger.error("Successor was changed, retry");
        continue;
      }

      ODHTNode precedingNode = nodeLookup.findById(precedingNodeAddress);
      if (precedingNode == null) {
        if (!precedingNodeAddress.equals(getSuccessor()))
          clearFingerPoint(precedingNodeAddress);
        else
          requestStabilization();

        continue;
      }

      try {
        final ONodeAddress successorResult = precedingNode.findSuccessor(id);
        logger.debug("Successor for id {} is {}", id, successorResult);
        return successorResult;
      } catch (ONodeOfflineException e) {
        if (!precedingNodeAddress.equals(getSuccessor()))
          clearFingerPoint(precedingNodeAddress);
        else
          requestStabilization();
      }
    }
  }

  private boolean isNodeOffline(ONodeAddress nodeAddress) {
    return nodeLookup.findById(nodeAddress) == null;
  }

  private void clearFingerPoint(ONodeAddress nodeId) {
    synchronized (fingersLock) {
      fingerPointCycle: while (true) {
        for (int i = 1; i < fingerPoints.length(); i++) {
          final ONodeAddress finger = fingerPoints.get(i);
          if (finger != null && nodeId.equals(finger))
            if (!fingerPoints.compareAndSet(i, finger, null))
              continue fingerPointCycle;
        }
        return;
      }
    }
  }

  public ONodeAddress[] getSuccessors() {
    final ONodeAddress successorId = getSuccessor();
    if (successorId.equals(nodeAddress))
      return new ONodeAddress[0];

    final ONodeAddress[] localSuccessors = successorsList;
    ONodeAddress[] result = new ONodeAddress[localSuccessors.length + 1];
    result[0] = successorId;

    if (localSuccessors.length > 0)
      System.arraycopy(localSuccessors, 0, result, 1, localSuccessors.length);

    return result;
  }

  private ONodeAddress findClosestPrecedingFinger(ONodeId key) {
    final ODHTRingInterval ringInterval = new ODHTRingInterval(nodeAddress.getNodeId().add(ONodeId.ONE), key.subtract(ONodeId.ONE));
    for (int i = fingerPoints.length() - 1; i >= 0; i--) {
      final ONodeAddress fingerPoint = fingerPoints.get(i);
      if (fingerPoint != null && ringInterval.insideInterval(fingerPoint.getNodeId())) {
        return fingerPoint;
      }
    }

    return this.nodeAddress;
  }

  public ONodeAddress getSuccessor() {
    return fingerPoints.get(0);
  }

  public ONodeAddress getPredecessor() {
    return predecessor.get();
  }

  public Record createRecord(String data) {
    waitTillJoin();

    int retryCount = 0;

    while (true) {
      try {
        final ONodeId id = ONodeId.generateUniqueId();
        retryCount++;

        return createRecord(new ORecordId(1, new OClusterPositionNodeId(id)), data);
      } catch (ORecordDuplicatedException e) {
        // ignore
        if (retryCount >= MAX_RETRIES)
          throw e;
      }
    }
  }

  public Record createRecord(ORecordId recordId, String data) {
    Logger logger = LoggerFactory.getLogger(this.getClass().getName() + ".createRecord");

    waitTillJoin();

    int retryCount = 0;

    while (true) {
      logger.debug("Looking for successor for record {}", recordId);
      if (retryCount > 5)
        System.out.println("Test");

      final ONodeAddress successorAddress = findSuccessor(((OClusterPositionNodeId) recordId.clusterPosition).getNodeId());

      logger.debug("Successor for record {} is {}", recordId, successorAddress);
      retryCount++;

      if (successorAddress.equals(nodeAddress)) {
        return createRecordInNode(recordId, data);
      } else {
        final RemoteNodeCallResult<Record> result = remoteNodeCreate(recordId, data, retryCount, successorAddress);
        if (result.repeat) {
          logger.error("Successor {} for record {} is offline, retry.", successorAddress, recordId);
          requestStabilization();
          continue;
        }

        return result.data;
      }
    }
  }

  public Record getRecord(ORecordId recordId) {
    Logger logger = LoggerFactory.getLogger(this.getClass().getName() + ".getRecord");

    waitTillJoin();
    int retryCount = 0;

    while (true) {
      retryCount++;

      logger.debug("Looking for successor for node with id {}", recordId);
      final ONodeAddress successorAddress = findSuccessor(((OClusterPositionNodeId) recordId.clusterPosition).getNodeId());
      logger.debug("Successor for record {} is {}", recordId, successorAddress);

      if (successorAddress != nodeAddress) {
        final RemoteNodeCallResult<Record> remoteNodeGetResult = remoteNodeGet(recordId, successorAddress, retryCount);

        if (remoteNodeGetResult.repeat) {
          logger.error("Successor {} for record {} is offline, retry.", successorAddress, recordId);

          requestStabilization();
          continue;
        }

        return remoteNodeGetResult.data;
      }

      return getRecordFromNode(recordId, true);
    }
  }

  @Override
  public Record createRecordInNode(ORecordId recordId, String data) {
    waitTillJoin();

    final Record result = addData(recordId, data);
    replicateRecord(result);

    return result;
  }

  @Override
  public void updateRecordInNode(ORecordId recordId, Record record) {
    synchronizeReplicas(recordId, syncReplicaCount);

    updateData(recordId, record);

    replicateRecord(recordId);
    startReadRepair(recordId);
  }

  @Override
  public void deleteRecordFromNode(ORecordId recordId, ODHTRecordVersion version) {
    synchronizeReplicas(recordId, syncReplicaCount);

    removeData(recordId, version);

    replicateRecord(recordId);
    startReadRepair(recordId);
  }

  private void synchronizeReplicas(ORecordId recordId, int syncReplicaCount) {
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

      logger.debug("Find replica holders for node {} replica count is {}", nodeAddress, replicaCount);

      final ONodeAddress[] replicaHolders = getSuccessors();
      logger.debug("Replica holders for node {} are {}", nodeAddress, replicaHolders);

      if (replicaHolders == null || replicaHolders.length == 0)
        return;

      final Set<ONodeAddress> holdersToReplicate = chooseReplicas(replicaHolders, syncReplicaCount);

      final Set<ONodeAddress> replicaHoldersToUpdate = new HashSet<ONodeAddress>();

      logger.debug("Replica holders for node {} are {}", nodeAddress, replicaHoldersToUpdate);

      for (ONodeAddress holderId : holdersToReplicate) {
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

              replicaHoldersToUpdate.add(primaryHolder.getNodeAddress());

              primaryMetadata = nodeMetadata;
              primaryHolder = holderNode;
            }
          } else {
            if (nodeMetadata != null) {
              final int cp = primaryMetadata.getVersion().compareTo(nodeMetadata.getVersion());

              if (cp < 0) {
                replicaHoldersToUpdate.add(primaryHolder.getNodeAddress());

                primaryMetadata = nodeMetadata;
                primaryHolder = holderNode;

                logger.debug("Primary metadata is not null but node metadata is more up to date  so we replace it."
                    + " record id {} node metadata {} primary metadata {}",
                    new Object[] { recordId, nodeMetadata, primaryMetadata });
              } else if (cp > 0) {
                logger.debug("Primary metadata is not null but node metadata out of date so we replace it. record id {}"
                    + " node metadata {} primary metadata {}", new Object[] { recordId, nodeMetadata, primaryMetadata });
                replicaHoldersToUpdate.add(holderNode.getNodeAddress());
              }
            } else {
              logger.debug("Node metadata is null but primary metadata is not so we replace it. record id {}"
                  + " node metadata {} primary metadata {}", new Object[] { recordId, nodeMetadata, primaryMetadata });
              replicaHoldersToUpdate.add(holderNode.getNodeAddress());
            }
          }

        } catch (Exception e) {
          // ignore
          logger.error("Exception during synchronization of record " + recordId + " for node " + holderId, e);
        }
      }

      logger.debug("Replica holders to update for record {}", recordId);

      if (!replicaHoldersToUpdate.isEmpty()) {
        logger.debug("Getting record from {} with id {}", primaryHolder.getNodeAddress(), recordId);

        Record result;
        if (nodeAddress.equals(primaryHolder.getNodeAddress()))
          result = readData(recordId);
        else
          try {
            result = primaryHolder.getRecordFromNode(recordId, false);
          } catch (Exception e) {
            continue;
          }

        logger.debug("Record with id {} was returned from {}" + " with content {}" + result,
            new Object[] { recordId, primaryHolder.getNodeAddress(), result });

        for (ONodeAddress replicaHolderAddress : replicaHoldersToUpdate) {
          ODHTNode replicaHolder = nodeLookup.findById(replicaHolderAddress);
          if (replicaHolder == null)
            continue;

          logger.debug("Holder with id {}" + " is absent during synchronization of record with id {}", replicaHolderAddress,
              recordId);

          try {
            logger.debug("Update of replica with {} for node {}", recordId, replicaHolder.getNodeAddress());
            replicaHolder.updateReplica(result, false);
            logger.debug("Replica with {} for node {} was updated.", recordId, replicaHolder.getNodeAddress());
          } catch (Exception e) {
            logger.error(
                "Exception during replication of record with id " + recordId + " for node " + replicaHolder.getNodeAddress(), e);
          }
        }
      }

      logger.debug("Synchronization of record with id {} was completed", recordId);
      return;
    }
  }

  private void startReadRepair(ORecordId recordId) {
    readRepairService.submit(new ReadRepairTask(recordId));
  }

  public Record getRecordFromNode(ORecordId recordId, boolean replicate) {
    if (replicate) {
      synchronizeReplicas(recordId, syncReplicaCount);
      startReadRepair(recordId);
    }

    return readData(recordId);
  }

  @Override
  public Record[] getRecordsFromNode(ORecordId[] ids, boolean replicate) {
    final ArrayList<Record> records = new ArrayList<Record>();
    for (ORecordId id : ids) {
      final Record record = getRecordFromNode(id, replicate);
      if (record != null)
        records.add(record);
    }

    Record[] recordsArray = new Record[records.size()];
    recordsArray = records.toArray(recordsArray);

    return recordsArray;
  }

  @Override
  public RecordMetadata getRecordMetadataFromNode(ORecordId id) {
    final Record record = readData(id);
    if (record == null)
      return null;

    return new RecordMetadata(record.getId(), record.getVersion());
  }

  public void updateRecord(ORecordId recordId, Record data) {
    waitTillJoin();
    int retryCount = 0;

    Logger logger = LoggerFactory.getLogger(this.getClass().getName() + ".updateRecord");
    while (true) {
      retryCount++;

      final ONodeAddress successorAddress = findSuccessor(((OClusterPositionNodeId) recordId.clusterPosition).getNodeId());

      if (!successorAddress.equals(nodeAddress)) {
        if (!remoteNodeUpdate(recordId, data, retryCount, successorAddress)) {
          logger.error("Successor {} for record {} is offline, retry.", successorAddress, recordId);
          requestStabilization();
          continue;
        }

        return;
      }

      updateRecordInNode(recordId, data);

      return;
    }
  }

  public void deleteRecord(ORecordId recordId, ODHTRecordVersion version) {
    waitTillJoin();
    int retryCount = 0;

    Logger logger = LoggerFactory.getLogger(this.getClass().getName() + ".deleteRecord");
    while (true) {
      retryCount++;

      final ONodeAddress successorAddress = findSuccessor(((OClusterPositionNodeId) recordId.clusterPosition).getNodeId());

      if (!successorAddress.equals(nodeAddress)) {
        if (!remoteNodeDelete(recordId, version, retryCount, successorAddress)) {
          logger.error("Successor {} for record {} is offline, retry.", successorAddress, recordId);
          requestStabilization();
          continue;
        }

        return;
      }

      deleteRecordFromNode(recordId, version);
      return;
    }
  }

  public ORecordId[] findMissedRecords(RecordMetadata[] recordMetadatas) {
    ArrayList<ORecordId> result = new ArrayList<ORecordId>();

    for (RecordMetadata recordMetadata : recordMetadatas) {
      final Record record = db.get(recordMetadata.getId());

      if (record == null)
        result.add(recordMetadata.getId());

      else if (recordMetadata.getVersion().compareTo(record.getVersion()) > 0)
        result.add(recordMetadata.getId());
    }

    ORecordId[] missedRecords = new ORecordId[result.size()];
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
  public RecordMetadata[] getRecordsForIntervalFromNode(ORecordId startId, ORecordId endId) {
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

  private RemoteNodeCallResult<Record> remoteNodeCreate(ORecordId id, String data, int retryCount, ONodeAddress remoteNodeAddress) {
    Logger logger = LoggerFactory.getLogger(OLocalDHTNode.class);

    final ODHTNode node = nodeLookup.findById(remoteNodeAddress);

    if (node == null) {
      if (retryCount < MAX_RETRIES) {
        logger.debug("Node {} is offline, retry {}-d time.", remoteNodeAddress, retryCount);

        try {
          Thread.sleep(50);
        } catch (InterruptedException e) {
          logger.error("Thread execution was interrupted.", e);
          throw new ORemoteNodeCallException("Thread execution was interrupted.", e, remoteNodeAddress.getNodeId());
        }

        return new RemoteNodeCallResult<Record>(true, null);
      } else {
        logger.error("Node {} is offline, retry limit is reached.", remoteNodeAddress);
        throw new ONodeOfflineException("Node " + remoteNodeAddress + " is offline, retry limit is reached.", null,
            remoteNodeAddress.getNodeId());
      }
    }

    try {
      return new RemoteNodeCallResult<Record>(false, node.createRecordInNode(id, data));
    } catch (ONodeOfflineException ooe) {
      if (retryCount < MAX_RETRIES) {
        logger.debug("Node {} is offline, retry {}-d time.", remoteNodeAddress, retryCount);

        try {
          Thread.sleep(50);
        } catch (InterruptedException e) {
          logger.error("Thread execution was interrupted.", e);
          throw new ORemoteNodeCallException("Thread execution was interrupted.", e, remoteNodeAddress.getNodeId());
        }

        return new RemoteNodeCallResult<Record>(true, null);
      } else {
        logger.error("Node " + remoteNodeAddress + " is offline, retry limit is reached.");
        throw new ONodeOfflineException("Node " + remoteNodeAddress + " is offline, retry limit is reached.", null,
            remoteNodeAddress.getNodeId());
      }
    }
  }

  private boolean remoteNodeUpdate(ORecordId id, Record data, int retryCount, ONodeAddress remoteNodeAddress) {
    Logger logger = LoggerFactory.getLogger(OLocalDHTNode.class);

    final ODHTNode node = nodeLookup.findById(remoteNodeAddress);

    if (node == null) {
      if (retryCount < MAX_RETRIES) {
        logger.debug("Node {} is offline, retry {}-d time.", remoteNodeAddress, retryCount);

        try {
          Thread.sleep(50);
        } catch (InterruptedException e) {
          logger.error("Thread execution was interrupted.", e);
          throw new ORemoteNodeCallException("Thread execution was interrupted.", e, remoteNodeAddress.getNodeId());
        }

        return false;
      } else {
        logger.error("Node " + remoteNodeAddress + " is offline, retry limit is reached.");
        throw new ONodeOfflineException("Node " + remoteNodeAddress + " is offline, retry limit is reached.", null,
            remoteNodeAddress.getNodeId());
      }
    }

    try {
      node.updateRecordInNode(id, data);
    } catch (ONodeOfflineException ooe) {
      if (retryCount < MAX_RETRIES) {
        logger.debug("Node {} is offline, retry {}-d time.", remoteNodeAddress, retryCount);

        try {
          Thread.sleep(50);
        } catch (InterruptedException e) {
          logger.error("Thread execution was interrupted.", e);
          throw new ORemoteNodeCallException("Thread execution was interrupted.", e, remoteNodeAddress.getNodeId());
        }

        return false;
      } else {
        logger.error("Node " + remoteNodeAddress + " is offline, retry limit is reached.");
        throw new ONodeOfflineException("Node " + remoteNodeAddress + " is offline, retry limit is reached.", null,
            remoteNodeAddress.getNodeId());
      }
    }

    return true;
  }

  private boolean remoteNodeDelete(ORecordId id, ODHTRecordVersion version, int retryCount, ONodeAddress remoteNodeAddress) {
    Logger logger = LoggerFactory.getLogger(OLocalDHTNode.class);

    final ODHTNode node = nodeLookup.findById(remoteNodeAddress);

    if (node == null) {
      if (retryCount < MAX_RETRIES) {
        logger.debug("Node {} is offline, retry {}-d time.", remoteNodeAddress, retryCount);

        try {
          Thread.sleep(50);
        } catch (InterruptedException e) {
          logger.error("Thread execution was interrupted.", e);
          throw new ORemoteNodeCallException("Thread execution was interrupted.", e, remoteNodeAddress.getNodeId());
        }

        return false;
      } else {
        logger.error("Node " + remoteNodeAddress + " is offline, retry limit is reached.");
        throw new ONodeOfflineException("Node " + remoteNodeAddress + " is offline, retry limit is reached.", null,
            remoteNodeAddress.getNodeId());
      }
    }

    try {
      node.deleteRecordFromNode(id, version);
    } catch (ONodeOfflineException ooe) {
      if (retryCount < MAX_RETRIES) {
        logger.debug("Node {} is offline, retry {}-d time.", remoteNodeAddress, retryCount);

        try {
          Thread.sleep(50);
        } catch (InterruptedException e) {
          logger.error("Thread execution was interrupted.", e);
          throw new ORemoteNodeCallException("Thread execution was interrupted.", e, remoteNodeAddress.getNodeId());
        }

        return false;
      } else {
        logger.error("Node " + remoteNodeAddress + " is offline, retry limit is reached.");
        throw new ONodeOfflineException("Node " + remoteNodeAddress + " is offline, retry limit is reached.", null,
            remoteNodeAddress.getNodeId());
      }
    }

    return true;
  }

  private Record addData(ORecordId id, String data) {
    lockManager.acquireLock(Thread.currentThread(), id, OLockManager.LOCK.EXCLUSIVE);
    try {
      return this.merkleTree.addData(id, data);
    } finally {
      lockManager.releaseLock(Thread.currentThread(), id, OLockManager.LOCK.EXCLUSIVE);
    }
  }

  private void updateData(ORecordId id, Record record) {
    lockManager.acquireLock(Thread.currentThread(), id, OLockManager.LOCK.EXCLUSIVE);
    try {
      this.merkleTree.updateData(id, record.getVersion(), record.getData());
    } finally {
      lockManager.releaseLock(Thread.currentThread(), id, OLockManager.LOCK.EXCLUSIVE);
    }
  }

  private RemoteNodeCallResult<Record> remoteNodeGet(ORecordId key, ONodeAddress remoteNodeAddress, int retryCount) {
    Logger logger = LoggerFactory.getLogger(OLocalDHTNode.class);

    ODHTNode node = nodeLookup.findById(remoteNodeAddress);
    if (node == null) {
      if (retryCount < MAX_RETRIES) {
        logger.debug("Node {} is offline, retry {}-d time.", remoteNodeAddress, retryCount);

        try {
          Thread.sleep(50);
        } catch (InterruptedException e) {
          logger.error("Thread execution was interrupted.", e);
          throw new ORemoteNodeCallException("Thread execution was interrupted.", e, remoteNodeAddress.getNodeId());
        }

        return new RemoteNodeCallResult<Record>(true, null);
      } else {
        logger.error("Node " + remoteNodeAddress + " is offline, retry limit is reached.");
        throw new ONodeOfflineException("Node " + remoteNodeAddress + " is offline, retry limit is reached.", null,
            remoteNodeAddress.getNodeId());
      }
    }

    try {
      return new RemoteNodeCallResult<Record>(false, node.getRecordFromNode(key, true));
    } catch (ONodeOfflineException e) {
      if (retryCount < MAX_RETRIES) {
        logger.debug("Node {} is offline, retry {}-d time.", remoteNodeAddress, retryCount);

        try {
          Thread.sleep(50);
        } catch (InterruptedException ie) {
          logger.error("Thread execution was interrupted.", ie);
          throw new ORemoteNodeCallException("Thread execution was interrupted.", ie, remoteNodeAddress.getNodeId());
        }

        return new RemoteNodeCallResult<Record>(true, null);
      } else {
        logger.error("Node " + remoteNodeAddress + " is offline, retry limit is reached.");
        throw e;
      }
    }
  }

  private void waitTillJoin() {
    Logger logger = LoggerFactory.getLogger(OLocalDHTNode.class);

    while (!NodeState.PRODUCTION.equals(state)) {
      logger.info("Wait till node {} will be joined.", nodeAddress);

      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException(e);
      }
    }
  }

  private Record readData(ORecordId dataId) {
    Record data;
    lockManager.acquireLock(Thread.currentThread(), dataId, OLockManager.LOCK.SHARED);
    try {
      data = db.get(dataId);
    } finally {
      lockManager.releaseLock(Thread.currentThread(), dataId, OLockManager.LOCK.SHARED);
    }
    return data;
  }

  private void putReplica(ORecordId id, Record replica) {
    lockManager.acquireLock(Thread.currentThread(), id, OLockManager.LOCK.SHARED);
    try {
      this.merkleTree.updateReplica(id, replica);
    } finally {
      lockManager.releaseLock(Thread.currentThread(), id, OLockManager.LOCK.SHARED);
    }
  }

  private void removeData(ORecordId id, ODHTRecordVersion version) {
    lockManager.acquireLock(Thread.currentThread(), id, OLockManager.LOCK.EXCLUSIVE);
    try {
      merkleTree.deleteData(id, version);
    } finally {
      lockManager.releaseLock(Thread.currentThread(), id, OLockManager.LOCK.EXCLUSIVE);
    }
  }

  private void cleanOutData(ORecordId id, ODHTRecordVersion version) {
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
        final ONodeAddress successorId = fingerPoints.get(0);

        ODHTNode successor = nodeLookup.findById(successorId);
        if (successor == null) {
          handleSuccessorOfflineCase(retryCount, successorId);

          retryCount++;
          result = false;
          continue;
        }

        final ONodeAddress predecessor;

        try {
          predecessor = successor.getPredecessor();
        } catch (ONodeOfflineException ooe) {
          handleSuccessorOfflineCase(retryCount, successorId);

          retryCount++;
          result = false;
          continue;
        }

        final ODHTRingInterval ringInterval = new ODHTRingInterval(this.nodeAddress.getNodeId().add(ONodeId.ONE), successorId
            .getNodeId().subtract(ONodeId.ONE));

        if (predecessor != null && ringInterval.insideInterval(predecessor.getNodeId())) {
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

        ONodeAddress prevPredecessor = null;
        try {
          if (!successor.equals(nodeAddress))
            prevPredecessor = successor.notifyParent(nodeAddress);
        } catch (ONodeOfflineException ooe) {
          handleSuccessorOfflineCase(retryCount, successor.getNodeAddress());

          retryCount++;
          result = false;
          continue;
        }

        if (prevPredecessor != null) {
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
          handleSuccessorOfflineCase(retryCount, successor.getNodeAddress());

          retryCount++;
          result = false;
        }
      }
    } catch (Exception e) {
      logger.error("Error during node stabilization.", e);
    }
  }

  private void updateSuccessors(ODHTNode successor) {
    final ONodeAddress successorAddress = successor.getNodeAddress();

    sListCycle: while (true) {
      final int successorsSize = Math.max(((int) Math.ceil(Math.log(nodeLookup.size()) / Math.log(2))), replicaCount);

      final List<ONodeAddress> processedSuccessors = new ArrayList<ONodeAddress>();
      ODHTNode dhtNode = successor;

      while (processedSuccessors.size() < successorsSize)
        try {
          final ONodeAddress nodeSuccessor = dhtNode.findSuccessor(dhtNode.getNodeAddress().getNodeId().add(ONodeId.ONE));
          if (nodeSuccessor == nodeAddress)
            break;

          dhtNode = nodeLookup.findById(nodeSuccessor);

          if (dhtNode == null)
            continue sListCycle;

          processedSuccessors.add(nodeSuccessor);
        } catch (ONodeOfflineException e) {
          if (!dhtNode.getNodeAddress().equals(successorAddress))
            continue sListCycle;
        }

      ONodeAddress[] newSuccessors = new ONodeAddress[processedSuccessors.size()];

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

    ONodeAddress[] replicaHolders = getSuccessors();

    logger.debug("Replica holders for record {} are {}", record.getId(), replicaHolders);

    if (replicaHolders == null || replicaHolders.length == 0)
      return;

    final Set<ONodeAddress> asyncReplicas = chooseReplicas(replicaHolders, replicaCount - syncReplicaCount);

    logger.debug("Async replica holders for record {} are {}", record.getId(), asyncReplicas);

    int processedReplicaHolders = 0;
    for (ONodeAddress replicaHolderAddress : replicaHolders) {
      final ODHTNode replicaHolderNode = nodeLookup.findById(replicaHolderAddress);
      if (replicaHolderNode == null) {
        logger.error("Replica holder with id " + replicaHolderAddress + " is absent.");
        continue;
      }

      try {
        boolean async = asyncReplicas.contains(replicaHolderAddress);

        logger.debug("Replication of record {} with async flag is set to {} to holder {}", new Object[] { record.getId(), async,
            replicaHolderAddress });

        replicaHolderNode.updateReplica(record, async);

        logger.debug("Replication of record {} with async flag is set to {} to holder {}  was finished.",
            new Object[] { record.getId(), async, replicaHolderAddress });

      } catch (Exception e) {
        logger.error("Exception during replication of record " + record.getId() + " to node " + replicaHolderAddress, e);
        // ignore
      }
      processedReplicaHolders++;
      if (processedReplicaHolders >= replicaCount)
        break;
    }

    logger.debug("Replication of record {} was finished.", record.getId());
  }

  private Set<ONodeAddress> chooseReplicas(ONodeAddress[] replicaHolders, int replicaCount) {
    replicaCount = Math.min(replicaCount, replicaHolders.length);

    final Set<ONodeAddress> replicas = new HashSet<ONodeAddress>();

    int holderIndex = 0;
    while (replicas.size() < replicaCount) {
      replicas.add(replicaHolders[holderIndex]);
      holderIndex++;
    }

    return replicas;
  }

  private void replicateRecord(ORecordId id) {
    final Record replica = readData(id);
    replicateRecord(replica);
  }

  private void handleSuccessorOfflineCase(int retryCount, ONodeAddress successorAddress) throws InterruptedException {
    Logger logger = LoggerFactory.getLogger(OLocalDHTNode.class);

    if (retryCount < MAX_RETRIES) {
      logger.debug("Successor {} is offline will try to find new one and retry. {}-d retry.", successorAddress, retryCount);

      Thread.sleep(50);

      final ONodeAddress newSuccessorAddress = findSuccessor(nodeAddress.getNodeId().add(ONodeId.ONE));

      if (fingerPoints.compareAndSet(0, successorAddress, newSuccessorAddress)) {
        final ODHTNode newSuccessorNode = nodeLookup.findById(newSuccessorAddress);
        if (newSuccessorNode != null)
          try {
            newSuccessorNode.notifyParent(nodeAddress);
          } catch (ONodeOfflineException noe) {
            fingerPoints.compareAndSet(0, newSuccessorAddress, successorAddress);
          }
      }
    } else {
      logger.error("Successor " + successorAddress + " for node " + nodeAddress + " is offline. Max retry count is reached.");
      throw new ONodeOfflineException("Successor " + successorAddress + " is offline for node " + nodeAddress
          + "  will try to find new one and retry." + " Max retry count is reached.", null, successorAddress.getNodeId());
    }
  }

  public void fixFingers() {
    synchronized (fingersLock) {
      final Logger logger = LoggerFactory.getLogger(this.getClass().getName() + ".fixPredecessor");
      try {
        int prevIndex = nextIndex - 1;
        ONodeAddress prevFinger = null;

        while (prevFinger == null && prevIndex >= 0) {
          prevFinger = fingerPoints.get(prevIndex);
          prevIndex--;
        }

        final ONodeAddress fingerPointCandidate = findSuccessor(nodeAddress.getNodeId().add(ONodeId.ONE).shiftLeft(nextIndex));

        if (prevFinger != null) {
          final ODHTRingInterval allowedInterval = new ODHTRingInterval(prevFinger.getNodeId().add(ONodeId.ONE), nodeAddress
              .getNodeId().subtract(ONodeId.ONE));
          if (!allowedInterval.insideInterval(fingerPointCandidate.getNodeId())) {
            nextIndex = 1;
            return;
          }

        }

        fingerPoints.set(nextIndex, fingerPointCandidate);

        nextIndex++;

        if (nextIndex > fingerPoints.length())
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
        ONodeAddress predecessorId = predecessor.get();

        if (predecessorId != null && nodeLookup.findById(predecessorId) == null) {
          result = predecessor.compareAndSet(predecessorId, null);
        } else
          result = true;
      }
    } catch (Exception e) {
      logger.error("Exception during predecessor fix", e);
    }
  }

  public ONodeAddress notifyParent(ONodeAddress predecessorCandidateId) {
    boolean result = false;
    ONodeAddress prevPredecessor = null;

    Logger logger = LoggerFactory.getLogger(this.getClass().getName() + ".notifyParent");

    while (!result) {
      ONodeAddress predecessorAddress = predecessor.get();

      final ODHTRingInterval ringInterval;
      if (predecessorAddress != null)
        ringInterval = new ODHTRingInterval(predecessorAddress.getNodeId().add(ONodeId.ONE), this.nodeAddress.getNodeId().subtract(
            ONodeId.ONE));
      else
        ringInterval = null;

      if (predecessorAddress == null || ringInterval.insideInterval(predecessorCandidateId.getNodeId())) {
        prevPredecessor = predecessorAddress;

        result = predecessor.compareAndSet(predecessorAddress, predecessorCandidateId);
        if (result)
          logger.info("New predecessor is {}", predecessorCandidateId);
        else {
          logger.info("Predecessor setup was failed.");
          prevPredecessor = null;
        }

        if (result && predecessorAddress == null && state == NodeState.JOIN) {
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

      builder.append(nodeAddress);
      ODHTNode node = this;

      Set<ONodeAddress> processedAddresses = new HashSet<ONodeAddress>();
      processedAddresses.add(nodeAddress);

      ONodeAddress successor = node.getSuccessor();
      while (!processedAddresses.contains(successor)) {
        builder.append("-").append(successor);
        processedAddresses.add(successor);

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

    private ONodeId      idToTest;

    private GlobalMaintenanceProtocol() {
      idToTest = nodeAddress.getNodeId();
    }

    public void run() {
      try {
        if (state == null || !state.equals(NodeState.PRODUCTION)) {
          logger.debug("Illegal state , wait till node will be ready to serve requests in DHT ring.");

          return;
        }

        logger.debug("Finding record with id next to {}", idToTest);
        final ORecordId nextRecordId = nextInDB(new ORecordId(1, new OClusterPositionNodeId(idToTest)));
        if (nextRecordId == null) {
          logger.debug("There are no records with id next to {}", idToTest);
          return;
        }

        ONodeId nextId = ((OClusterPositionNodeId) nextRecordId.clusterPosition).getNodeId();
        logger.debug("Record with id {} is closest successor for id {}", nextId, idToTest);

        logger.debug("Finding successor for record {}", nextId);
        ONodeAddress successor = findSuccessor(nextId);

        logger.debug("Successor for record is {}", successor);
        if (nodeAddress.equals(successor)) {
          idToTest = nodeAddress.getNodeId();

          logger.debug("We are owner of {} record. So we start from the beginning", nextId);
          return;
        }

        final ODHTNode successorNode = nodeLookup.findById(successor);
        if (successorNode == null) {
          idToTest = nodeAddress.getNodeId();

          logger.debug("Successor with id {} is absent, starting from beginning", successor);
          return;
        }

        logger.debug("Find the successors for node {}", successor);

        ONodeAddress[] successors = successorNode.getSuccessors();
        if (successors.length > replicaCount) {
          final ONodeAddress[] oldSuccessors = successors;
          successors = new ONodeAddress[replicaCount];

          System.arraycopy(oldSuccessors, 0, successors, 0, successors.length);
        }

        logger.debug("Successors list for node {} is {}", successor, successors);

        for (ONodeAddress s : successors) {
          if (s.equals(nodeAddress)) {
            idToTest = nodeAddress.getNodeId();

            logger.debug("We are owner of {} record. So we start from the beginning", nextId);

            return;
          }
        }

        List<ONodeAddress> nodesToReplicate = new ArrayList<ONodeAddress>();
        nodesToReplicate.add(successor);
        Collections.addAll(nodesToReplicate, successors);

        logger.debug("List of nodes to replicate records starting from {} to {} is {}", new Object[] { nextId, successor,
            nodesToReplicate });

        final Iterator<RecordMetadata> iterator = new ODHTRingIterator(db, new ORecordId(1, new OClusterPositionNodeId(idToTest)),
            new ORecordId(1, new OClusterPositionNodeId(successor.getNodeId())));

        final List<RecordMetadata> recordMetadatas = new ArrayList<RecordMetadata>(64);

        while (iterator.hasNext()) {
          final RecordMetadata recordMetadata = iterator.next();

          recordMetadatas.add(recordMetadata);

          if (recordMetadatas.size() >= 64)
            cleanOutForeignRecords(recordMetadatas, nodesToReplicate);
        }

        if (!recordMetadatas.isEmpty())
          cleanOutForeignRecords(recordMetadatas, nodesToReplicate);

        idToTest = successor.getNodeId();
      } catch (Exception e) {
        logger.error(e.toString(), e);

        idToTest = nodeAddress.getNodeId();
      }
    }

    private void cleanOutForeignRecords(List<RecordMetadata> recordMetadatas, List<ONodeAddress> nodesToReplicate) {
      RecordMetadata[] metadatas = new RecordMetadata[recordMetadatas.size()];
      metadatas = recordMetadatas.toArray(metadatas);

      for (ONodeAddress replicaHolderAddress : nodesToReplicate) {
        final ODHTNode node = nodeLookup.findById(replicaHolderAddress);

        if (node == null) {
          logger.debug("Node with id {} is absent. Continue replication with other node.", replicaHolderAddress);
          continue;
        }

        try {
          logger.debug("Finding missing ids for records with metadata {}", recordMetadatas);

          final ORecordId[] missingIds = node.findMissedRecords(metadatas);

          logger.debug("Missing ids are {}", missingIds);

          for (ORecordId missingId : missingIds) {
            final Record replica = db.get(missingId);
            if (replica != null)
              node.updateReplica(replica, false);
          }

        } catch (ONodeOfflineException noe) {
          logger.debug("Node with id {} is absent. Continue replication with other node.", replicaHolderAddress);
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

    private ORecordId nextInDB(ORecordId id) {
      ORecordId result = db.higherKey(id);

      if (result != null)
        return result;

      if (id.compareTo(new ORecordId(1, new OClusterPositionNodeId(ONodeId.ZERO))) > 0)
        result = db.ceilingKey(new ORecordId(1, new OClusterPositionNodeId(ONodeId.ZERO)));

      if (result != null && result != id)
        return result;

      return null;
    }
  }

  private static final class GlobalMaintenanceProtocolThreadFactory implements ThreadFactory {
    private static final AtomicInteger counter = new AtomicInteger();

    private final ONodeAddress         nodeAddress;

    private GlobalMaintenanceProtocolThreadFactory(ONodeAddress nodeAddress) {
      this.nodeAddress = nodeAddress;
    }

    public Thread newThread(Runnable r) {
      final Thread thread = new Thread(r);

      thread.setName("Global Maintenance Protocol for node '" + nodeAddress + "' [" + counter.incrementAndGet() + "]");
      thread.setDaemon(true);

      return thread;
    }
  }

  private static final class LocalMaintenanceProtocolThreadFactory implements ThreadFactory {
    private static final AtomicInteger counter = new AtomicInteger();

    private final ONodeAddress         nodeAddress;

    private LocalMaintenanceProtocolThreadFactory(ONodeAddress nodeAddress) {
      this.nodeAddress = nodeAddress;
    }

    public Thread newThread(Runnable r) {
      final Thread thread = new Thread(r);

      thread.setName("Local Maintenance Protocol for node '" + nodeAddress + "' [" + counter.incrementAndGet() + "]");
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

        final ONodeAddress localPredecessor = predecessor.get();
        logger.debug("Predecessor of node is {}", localPredecessor);

        if (localPredecessor == null) {
          logger.debug("Predecessor for node {} is absent. Start from the beginning.", nodeAddress);
          return;
        }

        logger.debug("Retrieving successors for node {}", nodeAddress);
        ONodeAddress[] replicaHolderAddresses = getSuccessors();

        if (replicaHolderAddresses.length > replicaCount) {
          final ONodeAddress[] oldReplicaHolderIDs = replicaHolderAddresses;
          replicaHolderAddresses = new ONodeAddress[replicaCount];

          System.arraycopy(oldReplicaHolderIDs, 0, replicaHolderAddresses, 0, replicaHolderAddresses.length);
        }

        logger.debug("Replica holders for node {} are {}", nodeAddress, replicaHolderAddresses);

        for (ONodeAddress replicaHolderAddress : replicaHolderAddresses) {
          try {
            final ONodeId startId = localPredecessor.getNodeId().add(ONodeId.ONE);
            final ONodeId endId = nodeAddress.getNodeId();

            final List<ODetachedMerkleTreeNode> roots = getRootsForInterval(merkleTree, startId, endId);
            for (final ODetachedMerkleTreeNode rootNode : roots)
              synchronizeNode(rootNode, replicaHolderAddress);
          } catch (Exception e) {
            logger.error("Error during replication of content to node " + replicaHolderAddress, e);
          }
        }
      } catch (Exception e) {
        logger.error(e.toString(), e);
      }
    }

    private List<ODetachedMerkleTreeNode> getRootsForInterval(final OMerkleTree tree, final ONodeId startId, final ONodeId endId) {
      if (startId.compareTo(endId) <= 0)
        return tree.getRootNodesForInterval(startId, endId);

      final List<ODetachedMerkleTreeNode> result = new ArrayList<ODetachedMerkleTreeNode>();

      final List<ODetachedMerkleTreeNode> firstInterval = tree.getRootNodesForInterval(startId, ONodeId.MAX_VALUE);
      final List<ODetachedMerkleTreeNode> secondInterval = tree.getRootNodesForInterval(ONodeId.ZERO, endId);

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

    private void synchronizeNode(final ODetachedMerkleTreeNode localTreeNode, final ONodeAddress remoteNodeAddress) {
      if (localTreeNode == null)
        throw new NodeSynchronizationFailedException("Passed Local Merkle Tree node is null.");

      final ODHTNode remoteNode = nodeLookup.findById(remoteNodeAddress);

      if (remoteNode == null)
        throw new NodeSynchronizationFailedException("Node with id " + remoteNodeAddress + " is absent.");

      final ODetachedMerkleTreeNode remoteTreeNode = remoteNode.findMerkleTreeNode(localTreeNode);

      if (remoteTreeNode == null)
        throw new NodeSynchronizationFailedException("Related remote Merkle tree node is null.");

      compareNodes(localTreeNode, remoteTreeNode, remoteNodeAddress);

      final ONodeAddress localPredecessor = predecessor.get();
      if (localPredecessor == null)
        throw new NodeSynchronizationFailedException("Predecessor " + localPredecessor + " is absent.");

      final ODHTRingInterval nodeInterval = new ODHTRingInterval(localPredecessor.getNodeId().add(ONodeId.ONE),
          nodeAddress.getNodeId());

      if (!localTreeNode.isLeaf() && !remoteTreeNode.isLeaf()) {
        for (int i = 0; i < 64; i++) {

          ODetachedMerkleTreeNode childNode = merkleTree.getChildNode(localTreeNode, i);
          final ONodeId startNodeId = childNode.getStartId();
          final ONodeId endNodeId = childNode.getEndId();

          final ODHTRingInterval treeNodeInterval = new ODHTRingInterval(startNodeId, endNodeId);

          if (nodeInterval.intersection(treeNodeInterval) != null) {
            if (!Arrays.equals(childNode.getHash(), remoteTreeNode.getChildHash(i)))
              synchronizeNode(childNode, remoteNodeAddress);
          }
        }
      }
    }

    private void compareNodes(ODetachedMerkleTreeNode localNode, ODetachedMerkleTreeNode remoteNode, ONodeAddress remoteNodeAddress) {
      if (Arrays.equals(remoteNode.getHash(), localNode.getHash()))
        return;

      compareWithLocal(localNode, remoteNode, remoteNodeAddress);
      compareWithRemote(localNode, remoteNode, remoteNodeAddress);
    }

    private void compareWithRemote(ODetachedMerkleTreeNode localNode, ODetachedMerkleTreeNode remoteNode,
        ONodeAddress remoteNodeAddress) {
      final ONodeAddress localPredecessor = predecessor.get();

      if (localPredecessor == null)
        throw new NodeSynchronizationFailedException("Node predecessor is absent.");

      final ODHTNode remoteDHTNode = nodeLookup.findById(remoteNodeAddress);
      if (remoteDHTNode == null)
        throw new NodeSynchronizationFailedException("Remote node with id " + remoteNodeAddress + " is offline.");

      final ODHTRingInterval dhtNodeInterval = new ODHTRingInterval(localPredecessor.getNodeId().add(ONodeId.ONE),
          nodeAddress.getNodeId());
      final ODHTRingInterval treeNodeInterval = new ODHTRingInterval(localNode.getStartId(), localNode.getEndId());

      final ODHTRingInterval recordsInterval = dhtNodeInterval.intersection(treeNodeInterval);
      if (recordsInterval == null)
        return;

      ONodeId startId = recordsInterval.getStart();
      ONodeId endId = recordsInterval.getEnd();

      final ArrayList<Record> recordsToReplicate = new ArrayList<Record>();
      if (localNode.isLeaf()) {
        RecordMetadata[] nodeMetadatas = getRecordsForIntervalFromNode(new ORecordId(1, new OClusterPositionNodeId(startId)),
            new ORecordId(1, new OClusterPositionNodeId(endId)));

        while (nodeMetadatas.length > 0) {
          final ORecordId[] missedIds = remoteDHTNode.findMissedRecords(nodeMetadatas);
          for (ORecordId missedId : missedIds) {
            final Record record = db.get(missedId);
            if (record != null)
              recordsToReplicate.add(record);

            if (recordsToReplicate.size() >= 64) {
              sendRecords(recordsToReplicate, remoteNodeAddress);
              recordsToReplicate.clear();
            }

          }

          startId = ((OClusterPositionNodeId) nodeMetadatas[nodeMetadatas.length - 1].getId().clusterPosition).getNodeId().add(
              ONodeId.ONE);

          if (recordsInterval.insideInterval(startId))
            nodeMetadatas = getRecordsForIntervalFromNode(new ORecordId(1, new OClusterPositionNodeId(startId)), new ORecordId(1,
                new OClusterPositionNodeId(endId)));
        }

        if (!recordsToReplicate.isEmpty())
          sendRecords(recordsToReplicate, remoteNodeAddress);
      } else if (remoteNode.isLeaf()) {
        final Set<RecordMetadata> merkleTreeMetadataSet = new HashSet<RecordMetadata>();

        RecordMetadata[] recordMetadatas = getRecordsForIntervalFromNode(new ORecordId(1, new OClusterPositionNodeId(startId)),
            new ORecordId(1, new OClusterPositionNodeId(endId)));

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
              sendRecords(recordsToReplicate, remoteNodeAddress);
              recordsToReplicate.clear();
            }
          }
          startId = ((OClusterPositionNodeId) recordMetadatas[recordMetadatas.length - 1].getId().clusterPosition).getNodeId().add(
              ONodeId.ONE);

          if (recordsInterval.insideInterval(startId))
            recordMetadatas = getRecordsForIntervalFromNode(new ORecordId(1, new OClusterPositionNodeId(startId)), new ORecordId(1,
                new OClusterPositionNodeId(endId)));
        }

        if (!recordsToReplicate.isEmpty())
          sendRecords(recordsToReplicate, remoteNodeAddress);

      }
    }

    private void compareWithLocal(ODetachedMerkleTreeNode localNode, ODetachedMerkleTreeNode remoteNode,
        ONodeAddress remoteNodeAddress) {
      final ONodeAddress localPredecessor = predecessor.get();

      if (localPredecessor == null)
        throw new NodeSynchronizationFailedException("Node predecessor is absent.");

      final ODHTRingInterval dhtNodeInterval = new ODHTRingInterval(localPredecessor.getNodeId().add(ONodeId.ONE),
          nodeAddress.getNodeId());

      final ArrayList<ORecordId> recordsToFetch = new ArrayList<ORecordId>();

      if (remoteNode.isLeaf()) {
        for (int i = 0; i < remoteNode.getRecordsCount(); i++) {
          final RecordMetadata recordMetadata = remoteNode.getRecordMetadata(i);
          if (dhtNodeInterval.insideInterval(((OClusterPositionNodeId) recordMetadata.getId().clusterPosition).getNodeId())) {
            final Record dbRecord = db.get(recordMetadata.getId());
            if (dbRecord == null || dbRecord.getVersion().compareTo(recordMetadata.getVersion()) < 0)
              recordsToFetch.add(recordMetadata.getId());
          }

          if (recordsToFetch.size() >= 64) {
            fetchRecords(recordsToFetch, remoteNodeAddress);
            recordsToFetch.clear();
          }
        }

        if (!recordsToFetch.isEmpty())
          fetchRecords(recordsToFetch, remoteNodeAddress);

      } else if (localNode.isLeaf()) {
        final ODHTRingInterval treeNodeInterval = new ODHTRingInterval(localNode.getStartId(), localNode.getEndId());

        final ODHTRingInterval recordsInterval = dhtNodeInterval.intersection(treeNodeInterval);
        if (recordsInterval == null)
          return;

        ONodeId startId = recordsInterval.getStart();
        ONodeId endId = recordsInterval.getEnd();

        final ODHTNode remoteDHTNode = nodeLookup.findById(remoteNodeAddress);
        if (remoteDHTNode == null)
          throw new NodeSynchronizationFailedException("Remote node with id " + remoteNodeAddress + " is offline.");

        RecordMetadata[] nodeMetadatas = remoteDHTNode.getRecordsForIntervalFromNode(new ORecordId(1, new OClusterPositionNodeId(
            startId)), new ORecordId(1, new OClusterPositionNodeId(endId)));

        while (nodeMetadatas.length > 0) {
          for (RecordMetadata nodeMetadata : nodeMetadatas) {
            final Record dbRecord = db.get(nodeMetadata.getId());

            if (dbRecord == null || dbRecord.getVersion().compareTo(nodeMetadata.getVersion()) < 0)
              recordsToFetch.add(nodeMetadata.getId());

            if (recordsToFetch.size() >= 64) {
              fetchRecords(recordsToFetch, remoteNodeAddress);
              recordsToFetch.clear();
            }
          }

          startId = ((OClusterPositionNodeId) nodeMetadatas[nodeMetadatas.length - 1].getId().clusterPosition).getNodeId().add(
              ONodeId.ONE);

          if (recordsInterval.insideInterval(startId))
            nodeMetadatas = remoteDHTNode.getRecordsForIntervalFromNode(new ORecordId(1, new OClusterPositionNodeId(startId)),
                new ORecordId(1, new OClusterPositionNodeId(endId)));
        }

        if (!recordsToFetch.isEmpty())
          fetchRecords(recordsToFetch, remoteNodeAddress);
      }
    }

    private void fetchRecords(List<ORecordId> missedRecords, ONodeAddress remoteNodeId) {
      final ODHTNode remoteNode = nodeLookup.findById(remoteNodeId);
      if (remoteNode == null)
        throw new NodeSynchronizationFailedException("Node with id " + remoteNodeId + " is absent in ring.");

      final Logger logger = LoggerFactory.getLogger(LocalMaintenanceProtocol.class);
      logger.debug("Records with ids {} are missed in current node {} will get it from node {}", new Object[] { missedRecords,
          nodeAddress, remoteNodeId });

      final ORecordId[] missedRecordsArray = new ORecordId[missedRecords.size()];
      for (int i = 0; i < missedRecordsArray.length; i++)
        missedRecordsArray[i] = missedRecords.get(i);

      final Record[] replicas = remoteNode.getRecordsFromNode(missedRecordsArray, false);
      logger.debug("Replicas  {} were found for node {}", new Object[] { replicas, nodeAddress });

      for (Record replica : replicas) {
        putReplica(replica.getId(), replica);
        logger.debug("Replica with id {} was updated for node {}", new Object[] { replica.getId(), nodeAddress });
      }
    }

    private void sendRecords(List<Record> missedRecords, ONodeAddress remoteNodeId) {
      final ODHTNode remoteNode = nodeLookup.findById(remoteNodeId);
      if (remoteNode == null)
        throw new NodeSynchronizationFailedException("Node with id " + remoteNodeId + " is absent in ring.");

      final Logger logger = LoggerFactory.getLogger(LocalMaintenanceProtocol.class);
      logger.debug("Records  {} are missed in node {} will get it from current node {}", new Object[] { missedRecords,
          remoteNodeId, nodeAddress });

      Record[] missedRecordsArray = new Record[missedRecords.size()];
      missedRecordsArray = missedRecords.toArray(missedRecordsArray);

      remoteNode.updateReplicas(missedRecordsArray, false);

      logger.debug("Replicas {} were updated for node {}", new Object[] { missedRecords, remoteNodeId });
    }
  }

  private static final class ReadRepairThreadFactory implements ThreadFactory {
    private static final AtomicInteger counter = new AtomicInteger();

    private final ONodeAddress         nodeAddress;

    private ReadRepairThreadFactory(ONodeAddress nodeAddress) {
      this.nodeAddress = nodeAddress;
    }

    public Thread newThread(Runnable r) {
      final Thread thread = new Thread(r);

      thread.setName("Read Repair Protocol for node '" + nodeAddress + "' [" + counter.incrementAndGet() + "]");
      thread.setDaemon(true);

      return thread;
    }
  }

  private final class ReadRepairTask implements Callable<Void> {
    private final Logger    logger = LoggerFactory.getLogger(ReadRepairTask.class);
    private final ORecordId recordId;

    private ReadRepairTask(ORecordId recordId) {
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
