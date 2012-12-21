package com.orientechnologies.orient.server.distributed;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceArray;

import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.version.ORecordVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.orientechnologies.common.concur.lock.OLockManager;
import com.orientechnologies.orient.core.id.ONodeId;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.server.distributed.merkletree.ODetachedMerkleTreeNode;
import com.orientechnologies.orient.server.distributed.merkletree.OInMemoryMerkleTree;
import com.orientechnologies.orient.server.distributed.merkletree.OMerkleTree;
import com.orientechnologies.orient.server.distributed.operations.ODistributedCoordinatorFactory;
import com.orientechnologies.orient.server.distributed.operations.ODistributedRecordCreation;
import com.orientechnologies.orient.server.distributed.operations.ODistributedRecordDelete;
import com.orientechnologies.orient.server.distributed.operations.ODistributedRecordOperationCoordinator;
import com.orientechnologies.orient.server.distributed.operations.ODistributedRecordRead;
import com.orientechnologies.orient.server.distributed.operations.ODistributedRecordUpdate;
import com.orientechnologies.orient.server.distributed.ringprotocols.ORingProtocolsFactory;
import com.orientechnologies.orient.server.distributed.ringprotocols.crud.ORecordCreator;
import com.orientechnologies.orient.server.distributed.ringprotocols.crud.ORecordDeleter;
import com.orientechnologies.orient.server.distributed.ringprotocols.crud.ORecordReader;
import com.orientechnologies.orient.server.distributed.ringprotocols.crud.ORecordUpdater;

/**
 * @author Andrey Lomakin
 * @since 17.08.12
 */
public final class OLocalDHTNode implements ODHTNode, ODHTNodeLocal {
  private static final int                             MAX_RETRIES           = 10;
  private static final int                             MAX_RECORDS_TO_RETURN = 64;

  private final ONodeAddress                           nodeAddress;

  private final AtomicReferenceArray<ONodeAddress>     fingerPoints          = new AtomicReferenceArray<ONodeAddress>(
                                                                                 ONodeId.NODE_SIZE_BITS - 1);
  private AtomicReference<ONodeAddress>                predecessor           = new AtomicReference<ONodeAddress>();

  private final ODHTNodeLookup                         nodeLookup;

  private int                                          nextIndex             = 0;
  private final Object                                 fingersLock           = new Object();

  private final OLockManager<ORID, Runnable>           lockManager           = new OLockManager<ORID, Runnable>(true, 500);

  private volatile ONodeAddress[]                      successorsList        = new ONodeAddress[0];

  private volatile NodeState                           state;

  private final OMerkleTree                            merkleTree;

  private final int                                    replicaCount;
	private final int 																	 syncReplicaCount;

  private final ODistributedRecordOperationCoordinator operationCoordinator;

  private final ORecordCreator                         recordCreator;
  private final ORecordUpdater                         recordUpdater;
  private final ORecordDeleter                         recordDeleter;
  private final ORecordReader                          recordReader;
	private final ODHTDatabaseLookup                     databaseLookup;

  public OLocalDHTNode(ONodeAddress nodeAddress,
											 ODHTNodeLookup nodeLookup,
											 ODHTDatabaseLookup databaseLookup,
											 ODistributedCoordinatorFactory distributedCoordinatorFactory,
											 ORingProtocolsFactory ringProtocolsFactory,
											 int replicaCount, int syncReplicaCount) {

    this.nodeAddress = nodeAddress;

    this.replicaCount = replicaCount;
    this.nodeLookup = nodeLookup;
		this.syncReplicaCount = syncReplicaCount;
		this.databaseLookup = databaseLookup;

		this.operationCoordinator = distributedCoordinatorFactory.createOperationCoordinator(nodeLookup);

    this.recordCreator = ringProtocolsFactory.createRecordCreator(nodeLookup, replicaCount, syncReplicaCount);
    this.recordUpdater = ringProtocolsFactory.createRecordUpdater(nodeLookup, replicaCount, syncReplicaCount);
    this.recordReader = ringProtocolsFactory.createRecordReader(nodeLookup, replicaCount, syncReplicaCount);
    this.recordDeleter = ringProtocolsFactory.createRecordDeleter(nodeLookup, replicaCount, syncReplicaCount);

    this.merkleTree = new OInMemoryMerkleTree(databaseLookup, 1);
  }

  public ODatabaseRecord getDb(String storageName) {
    return databaseLookup.openDatabase(storageName);
  }

  public void createDHT() {
    Logger logger = LoggerFactory.getLogger(OLocalDHTNode.class);
    logger.info("New ring creation was started");

    fingerPoints.set(0, nodeAddress);
    state = NodeState.PRODUCTION;

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
              .debug("Id {} inside interval {} - {} ", new Object[] { id, nodeAddress.getNodeId(), successorAddress.getNodeId() });
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

  public ORecordInternal<?> crateRecord(String storageName, ORecordInternal<?> record) {
    return operationCoordinator.executeRecordOperation(this, new ODistributedRecordCreation(storageName, record));
  }

  public ORecordInternal<?> createRecord(String storageName, ORecordInternal<?> record) {
    return operationCoordinator.executeRecordOperation(this, new ODistributedRecordCreation(storageName, record));
  }

  public ORecordInternal<?> readRecord(String storageName, ORID recordId) {
    return operationCoordinator.executeRecordOperation(this, new ODistributedRecordRead(storageName, recordId));
  }

  public ORecordInternal<?> updateRecord(String storageName, ORecordInternal<?> record) {
    return operationCoordinator.executeRecordOperation(this, new ODistributedRecordUpdate(storageName, record));
  }

  public void deleteRecord(String storageName, ORID recordId, ORecordVersion version) {
    operationCoordinator.executeRecordOperation(this, new ODistributedRecordDelete(storageName, recordId, version));
  }

  @Override
  public ORecordInternal<?> createRecordInNode(String storageName, ORecordInternal<?> record) {
    return recordCreator.createRecord(storageName, this, record, replicaCount, syncReplicaCount);
  }

  @Override
  public ORecordInternal<?> updateRecordInNode(String storageName, ORecordInternal<?> record) {
    return recordUpdater.updateRecord(storageName, this, record, replicaCount, syncReplicaCount);
  }

  @Override
  public void deleteRecordFromNode(String storageName, ORID recordId, ORecordVersion version) {
    recordDeleter.deleteRecord(storageName, this, recordId, version, replicaCount, syncReplicaCount);
  }

  public ORecordInternal<?> readRecordFromNode(String storageName, ORID recordId) {
    return recordReader.readRecord(storageName, this, recordId, replicaCount, syncReplicaCount);
  }

  public ORecordInternal<?> getRecordFromNode(String storageName, ORID recordId) {
    return readRecordLocal(storageName, recordId);
  }

  @Override
  public ORecordInternal<?>[] getRecordsFromNode(String storageName, ORID[] ids) {
    final ArrayList<ORecordInternal<?>> records = new ArrayList<ORecordInternal<?>>();
    for (ORID id : ids) {
      final ORecordInternal<?> record = getRecordFromNode(storageName, id);
      if (record != null)
        records.add(record);
    }

    ORecordInternal[] recordsArray = new ORecordInternal[records.size()];
    recordsArray = records.toArray(recordsArray);

    return recordsArray;
  }

  @Override
  public ORecordMetadata getRecordMetadataFromNode(String storageName, ORID id) {
    final ORecordInternal record = readRecordLocal(storageName, id);
    if (record == null)
      return null;

    return new ORecordMetadata(record.getIdentity(), record.getRecordVersion());
  }

  public ORID[] findMissedRecords(String storageName, ORecordMetadata[] recordMetadatas) {
    ArrayList<ORID> result = new ArrayList<ORID>();

    final ODatabaseRecord db = databaseLookup.openDatabase(storageName);

    for (ORecordMetadata recordMetadata : recordMetadatas) {
      final ORecordInternal<?> record = db.getRecord(recordMetadata.getRid());

      if (record == null)
        result.add(recordMetadata.getRid());

      else if (recordMetadata.getVersion().compareTo(record.getRecordVersion()) > 0)
        result.add(recordMetadata.getRid());
    }

    ORID[] missedRecords = new ORID[result.size()];
    for (int i = 0; i < missedRecords.length; i++)
      missedRecords[i] = result.get(i);

    return missedRecords;
  }

  @Override
  public void updateReplicas(String storageName, ORecordInternal<?>[] replicas, boolean async) {
    for (ORecordInternal<?> replica : replicas)
      updateReplica(storageName, replica, async);
  }

  public void updateReplica(String storageName, final ORecordInternal<?> replica, final boolean async) {
    putReplica(replica.getIdentity(), replica);
  }

  @Override
  public ORecordMetadata[] getRecordsForIntervalFromNode(String storageName, ORID startId, ORID endId) {
    final List<ORecordMetadata> recordMetadatas = new ArrayList<ORecordMetadata>();

    int processedRecords = 0;

    final ODatabaseRingIterator ringIterator = new ODatabaseRingIterator(databaseLookup.openDatabase(storageName), startId, endId);
    while (ringIterator.hasNext()) {
      final ORecordMetadata recordMetadata = ringIterator.next();
      if (recordMetadata != null)
        recordMetadatas.add(recordMetadata);

      processedRecords++;

      if (processedRecords >= MAX_RECORDS_TO_RETURN)
        break;
    }

    ORecordMetadata[] result = new ORecordMetadata[recordMetadatas.size()];
    result = recordMetadatas.toArray(result);

    return result;
  }

  @Override
  public ORecordInternal<?> addRecordLocal(String storageName, ORecordInternal<?> record) {
    final ORID id = record.getIdentity();
    lockManager.acquireLock(Thread.currentThread(), id, OLockManager.LOCK.EXCLUSIVE);
    try {
      return this.merkleTree.addData(id, record);
    } finally {
      lockManager.releaseLock(Thread.currentThread(), id, OLockManager.LOCK.EXCLUSIVE);
    }
  }

  @Override
  public void updateRecordLocal(String storageName, ORecordInternal<?> record) {
    final ORID id = record.getIdentity();
    lockManager.acquireLock(Thread.currentThread(), id, OLockManager.LOCK.EXCLUSIVE);
    try {
      this.merkleTree.updateData(id, record.getRecordVersion(), record);
    } finally {
      lockManager.releaseLock(Thread.currentThread(), id, OLockManager.LOCK.EXCLUSIVE);
    }
  }

  @Override
  public ORecordInternal<?> readRecordLocal(String storageName, ORID dataId) {
    ORecordInternal<?> data;
    lockManager.acquireLock(Thread.currentThread(), dataId, OLockManager.LOCK.SHARED);
    try {
      data = databaseLookup.openDatabase(storageName).getRecord(dataId);
    } finally {
      lockManager.releaseLock(Thread.currentThread(), dataId, OLockManager.LOCK.SHARED);
    }
    return data;
  }

  private void putReplica(ORID id, ORecordInternal<?> replica) {
    lockManager.acquireLock(Thread.currentThread(), id, OLockManager.LOCK.SHARED);
    try {
      this.merkleTree.updateReplica(id, replica);
    } finally {
      lockManager.releaseLock(Thread.currentThread(), id, OLockManager.LOCK.SHARED);
    }
  }

  @Override
  public void removeRecordLocal(String storageName, ORID id, ORecordVersion version) {
    lockManager.acquireLock(Thread.currentThread(), id, OLockManager.LOCK.EXCLUSIVE);
    try {
      merkleTree.deleteData(id, version);
    } finally {
      lockManager.releaseLock(Thread.currentThread(), id, OLockManager.LOCK.EXCLUSIVE);
    }
  }

  @Override
  public OMerkleTree getLocalMerkleTree(String storageName, int clusterId) {
    return merkleTree;
  }

  public void cleanOutRecord(String storageName, ORID id, ORecordVersion version) {
    lockManager.acquireLock(Thread.currentThread(), id, OLockManager.LOCK.EXCLUSIVE);
    try {
      merkleTree.deleteData(id, version, false);
    } finally {
      lockManager.releaseLock(Thread.currentThread(), id, OLockManager.LOCK.EXCLUSIVE);
    }
  }

  @Override
  public ODatabaseRingIterator getLocalRingIterator(String storageName, ORID startRid, ORID endId) {
    return new ODatabaseRingIterator(databaseLookup.openDatabase(storageName), startRid, endId);
  }

  public int size(String storageName, int clusterId) {
    int count = 0;

    final ODatabaseRecord db = databaseLookup.openDatabase(storageName);
    final String clusterName = db.getClusterNameById(clusterId);
    for (ORecordInternal<?> record : db.browseCluster(clusterName)) {
      if (!record.getRecordVersion().isTombstone())
        count++;
    }

    return count;
  }

  public NodeState state() {
    return state;
  }

	public void stop() {
		synchronized (fingersLock) {
			state = null;

			predecessor.set(null);
			for (int i = 0; i < fingerPoints.length(); i++) {
				fingerPoints.set(i, null);
			}

			successorsList  = new ONodeAddress[0];
		}
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
          if (nodeSuccessor.equals(nodeAddress))
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
  public ODetachedMerkleTreeNode findMerkleTreeNode(String storageName, ODetachedMerkleTreeNode remoteNode) {
    final ODetachedMerkleTreeNode localNode = merkleTree.getEquivalentNode(remoteNode);
    if (localNode == null)
      return null;

    return localNode;
  }
}
