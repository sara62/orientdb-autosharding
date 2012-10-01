package com.orientechnologies.orient.server.distributed;

import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

import com.orientechnologies.common.concur.lock.OLockManager;
import com.orientechnologies.common.util.MersenneTwister;
import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.storage.ORecordDuplicatedException;

/**
 * @author Andrey Lomakin
 * @since 17.08.12
 */
public class OLocalDHTNode implements ODHTNode {
  private static final int                   MAX_RETRIES    = 10;

  private final MersenneTwister              random         = new MersenneTwister();

  private final long                         id;

  private final AtomicLongArray              fingerPoints   = new AtomicLongArray(63);
  private AtomicLong                         predecessor    = new AtomicLong(-1);

  private final NavigableMap<Long, Record>   db             = new ConcurrentSkipListMap<Long, Record>();

  private volatile long                      migrationId    = -1;
  private volatile ODHTNodeLookup            nodeLookup;

  private AtomicInteger                      next           = new AtomicInteger(1);
  private final OLockManager<Long, Runnable> lockManager    = new OLockManager<Long, Runnable>(true, 500);
  private volatile long[]                    successorsList = new long[0];

  private final ExecutorService              gmExecutorService;

  private volatile NodeState                 state;

  private final OMerkleTree                  merkleTree     = new OMerkleTree(db);
  private final int                          replicaCount;

  public OLocalDHTNode(long id, int replicaCount) {
    this.id = id;
    for (int i = 0; i < fingerPoints.length(); i++)
      fingerPoints.set(i, -1);

    this.replicaCount = replicaCount;
    gmExecutorService = Executors.newSingleThreadExecutor(new GlobalMaintenanceProtocolThreadFactory(id));
  }

  public ODHTNodeLookup getNodeLookup() {
    return nodeLookup;
  }

  public void setNodeLookup(ODHTNodeLookup nodeLookup) {
    this.nodeLookup = nodeLookup;
  }

  public void create() {
    log("New ring creation was started");

    predecessor.set(-1);
    fingerPoints.set(0, id);
    state = NodeState.STABLE;

    log("New ring was created");
  }

  public long getNodeId() {
    return id;
  }

  public boolean join(long nodeId) {
    try {
      log("Join is started using node with id " + nodeId);

      final ODHTNode node = nodeLookup.findById(nodeId);
      if (node == null) {
        log("Node with id " + nodeId + " is absent.");
        return false;
      }

      if (state == null)
        gmExecutorService.submit(new GlobalMaintenanceProtocol());

      random.setSeed((new Random()).nextLong());

      state = NodeState.JOIN;

      predecessor.set(-1);
      int retryCount = 0;

      while (true) {
        try {
          final long successorId = node.findSuccessor(id);
          fingerPoints.set(0, successorId);

          ODHTNode successor = nodeLookup.findById(successorId);
          if (successor == null) {
            if (retryCount < MAX_RETRIES) {
              log("Node " + successorId + " is offline, retry " + retryCount + "-d time.");
              retryCount++;
              Thread.sleep(100);
            } else {
              log("Node " + successorId + " is offline, max retries is reached");
              return false;
            }

            continue;
          }

          final long prevPredecessor = successor.notifyParent(id);
          if (prevPredecessor > -1) {
            final ODHTNode prevPredecessorNode = nodeLookup.findById(prevPredecessor);
            if (prevPredecessorNode != null)
              try {
                prevPredecessorNode.requestStabilization();
              } catch (ONodeOfflineException e) {
                // ignore
              }
          }

          log("Join completed, successor is " + fingerPoints.get(0));

          return true;
        } catch (ONodeOfflineException ooe) {
          if (ooe.getNodeId() == nodeId) {
            log("Node with id " + nodeId + " is absent.");
            return false;
          }

          if (retryCount < MAX_RETRIES) {
            log("Node " + ooe.getNodeId() + " is offline, retry " + retryCount + "-d time.");
            retryCount++;
            Thread.sleep(100);
          } else {
            log("Node " + ooe.getNodeId() + " is offline, max retries is reached");
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
    while (true) {
      // log("Successor request for key " + key);
      final long successorId = fingerPoints.get(0);

      if (insideInterval(id, successorId, key, true)) {
        // log("Key " + key + " inside interval " + id + " " + successorId);
        return successorId;
      }

      long nodeId = findClosestPrecedingFinger(key);

      // log("Closest preceding node for key " + key + " is " + nodeId);

      ODHTNode node = nodeLookup.findById(nodeId);
      if (node == null) {
        if (nodeId != successorId) {
          clearFingerPoint(nodeId);
          continue;
        } else {
          final long[] successors = successorsList;
          for (final long successor : successors) {
            if (successor == id)
              return id;

            final ODHTNode successorNode = nodeLookup.findById(successor);
            if (successorNode != null) {
              try {
                return successorNode.findSuccessor(key);
              } catch (ONodeOfflineException noe) {
                log(noe.toString());
              }
            }
          }

          throw new ONodeOfflineException("Node " + nodeId + " is offline .", null, nodeId);
        }
      }

      try {
        final long successorResult = node.findSuccessor(key);
        // log("Successor for key " + key + " is " + successorResult);
        return successorResult;
      } catch (ONodeOfflineException e) {
        if (nodeId != successorId) {
          clearFingerPoint(nodeId);
        } else {
          final long[] successors = successorsList;
          for (final long successor : successors) {
            if (successor == id)
              return id;

            final ODHTNode successorNode = nodeLookup.findById(successor);
            if (successorNode != null) {
              try {
                return successorNode.findSuccessor(key);
              } catch (ONodeOfflineException noe) {
                log(noe.toString());
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

  public long[] getSuccessors(int depth) {
    if (depth == 0)
      return new long[] { fingerPoints.get(0) };

    ODHTNode node = nodeLookup.findById(fingerPoints.get(0));
    if (node == null)
      return null;

    try {
      long[] successors = new long[depth + 1];
      long[] result = node.getSuccessors(depth - 1);
      if (result == null)
        return null;

      System.arraycopy(result, 0, successors, 1, result.length);
      successors[0] = fingerPoints.get(0);

      return successors;
    } catch (ONodeOfflineException noe) {
      return null;
    }
  }

  private long findClosestPrecedingFinger(long key) {
    // log("Closest preceding request for key " + key);

    for (int i = fingerPoints.length() - 1; i >= 0; i--) {
      final long fingerPoint = fingerPoints.get(i);
      if (fingerPoint > -1 && insideInterval(this.id, key, fingerPoint, false)) {
        // log("Closest preceding finger for key " + key + " is " + fingerPoint);
        return fingerPoint;
      }
    }

    // log("Closest preceding finger for key " + key + " is " + this.id);

    return this.id;
  }

  public long getSuccessor() {
    return fingerPoints.get(0);
  }

  public Long getPredecessor() {
    return predecessor.get();
  }

  public Record create(String data) {
    waitTillJoin();

    int retryCount = 0;

    while (true) {
      try {
        final long id = random.nextLong(Long.MAX_VALUE);
        retryCount++;

        return create(id, data);
      } catch (ORecordDuplicatedException e) {
        // ignore
        if (retryCount >= MAX_RETRIES)
          throw e;
      }
    }
  }

  public Record create(long id, String data) {
    waitTillJoin();

    int retryCount = 0;

    while (true) {
      final long successorId = findSuccessor(id);
      retryCount++;

      if (successorId == id) {
        if (state == NodeState.MERGING) {
          final Record mergeData = getDataFromMigrationNode(id);
          if (mergeData != null)
            putReplica(id, mergeData);
        }

        return addData(id, data);
      } else {
        final RemoteNodeCallResult<Record> result = remoteNodeCreate(id, data, retryCount, successorId);
        if (result.repeat)
          continue;

        return result.data;
      }
    }
  }

  public Record get(long id) {
    waitTillJoin();
    int retryCount = 0;

    while (true) {
      retryCount++;

      final long successorId = findSuccessor(id);
      if (successorId != id) {
        final RemoteNodeCallResult<Record> remoteNodeGetResult = remoteNodeGet(id, successorId, retryCount);

        if (remoteNodeGetResult.repeat)
          continue;

        return remoteNodeGetResult.data;
      }

      if (state == NodeState.MERGING) {
        final Record mergeData = getDataFromMigrationNode(id);
        if (mergeData != null)
          putReplica(id, mergeData);
      }

      return readData(id);
    }
  }

  public Record getRecordFromNode(long id) {
    Record mergeData;

    if (state == NodeState.MERGING)
      mergeData = getDataFromMigrationNode(id);
    else
      mergeData = null;

    Record record = readData(id);
    if (mergeData == null)
      return record;

    return record.compareVersions(mergeData) > 0 ? record : mergeData;
  }

  public void update(long id, Record data) {
    waitTillJoin();
    int retryCount = 0;

    while (true) {
      retryCount++;

      final long successorId = findSuccessor(id);
      if (successorId != id) {
        if (!remoteNodeUpdate(id, data, retryCount, successorId))
          continue;

        return;
      }

      if (state == NodeState.MERGING) {
        final Record mergeData = getDataFromMigrationNode(id);
        if (mergeData != null)
          putReplica(id, mergeData);
      }

      updateData(id, data);
    }
  }

  public void remove(long id, int version) {
    waitTillJoin();
    int retryCount = 0;

    while (true) {
      retryCount++;

      final long successorId = findSuccessor(id);
      if (successorId != id) {
        if (!remoteNodeRemove(id, version, retryCount, successorId))
          continue;

        return;
      }

      if (state == NodeState.MERGING) {
        final Record mergeData = getDataFromMigrationNode(id);
        if (mergeData != null)
          putReplica(id, mergeData);
      }

      removeData(id, version);
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

  public void updateReplica(final Record replica) {
    putReplica(replica.getId(), replica);
  }

  private RemoteNodeCallResult<Record> remoteNodeCreate(long id, String data, int retryCount, long nodeId) {
    final ODHTNode node = nodeLookup.findById(nodeId);

    if (node == null) {
      if (retryCount < MAX_RETRIES) {
        log("Node " + nodeId + " is offline, retry " + retryCount + "-d time.");
        return new RemoteNodeCallResult<Record>(true, null);
      } else {
        log("Node " + nodeId + " is offline, retry limit is reached.");
        throw new ONodeOfflineException("Node " + nodeId + " is offline, retry limit is reached.", null, nodeId);
      }
    }

    try {
      return new RemoteNodeCallResult<Record>(false, node.create(id, data));
    } catch (ONodeOfflineException ooe) {
      if (retryCount < MAX_RETRIES) {
        log("Node " + nodeId + " is offline, retry " + retryCount + "-d time.");
        return new RemoteNodeCallResult<Record>(true, null);
      } else {
        log("Node " + nodeId + " is offline, retry limit is reached.");
        throw new ONodeOfflineException("Node " + nodeId + " is offline, retry limit is reached.", null, nodeId);
      }
    }
  }

  private boolean remoteNodeUpdate(long id, Record data, int retryCount, long nodeId) {
    final ODHTNode node = nodeLookup.findById(nodeId);

    if (node == null) {
      if (retryCount < MAX_RETRIES) {
        log("Node " + nodeId + " is offline, retry " + retryCount + "-d time.");
        return false;
      } else {
        log("Node " + nodeId + " is offline, retry limit is reached.");
        throw new ONodeOfflineException("Node " + nodeId + " is offline, retry limit is reached.", null, nodeId);
      }
    }

    try {
      node.update(id, data);
    } catch (ONodeOfflineException ooe) {
      if (retryCount < MAX_RETRIES) {
        log("Node " + nodeId + " is offline, retry " + retryCount + "-d time.");
        return false;
      } else {
        log("Node " + nodeId + " is offline, retry limit is reached.");
        throw new ONodeOfflineException("Node " + nodeId + " is offline, retry limit is reached.", null, nodeId);
      }
    }

    return true;
  }

  private boolean remoteNodeRemove(long id, int version, int retryCount, long nodeId) {
    final ODHTNode node = nodeLookup.findById(nodeId);

    if (node == null) {
      if (retryCount < MAX_RETRIES) {
        log("Node " + nodeId + " is offline, retry " + retryCount + "-d time.");
        return false;
      } else {
        log("Node " + nodeId + " is offline, retry limit is reached.");
        throw new ONodeOfflineException("Node " + nodeId + " is offline, retry limit is reached.", null, nodeId);
      }
    }

    try {
      node.remove(id, version);
    } catch (ONodeOfflineException ooe) {
      if (retryCount < MAX_RETRIES) {
        log("Node " + nodeId + " is offline, retry " + retryCount + "-d time.");
        return false;
      } else {
        log("Node " + nodeId + " is offline, retry limit is reached.");
        throw new ONodeOfflineException("Node " + nodeId + " is offline, retry limit is reached.", null, nodeId);
      }
    }

    return true;
  }

  private Record getDataFromMigrationNode(long id) {
    final long migrationNodeId = migrationId;
    if (migrationNodeId < 0)
      return null;

    int retryCount = 0;

    while (true) {
      retryCount++;
      if (retryCount > replicaCount + 1) {
        state = NodeState.STABLE;
        migrationId = -1;

        return null;
      }

      ODHTNode node = nodeLookup.findById(migrationNodeId);
      if (node == null) {
        if (replicaCount < 1) {
          state = NodeState.STABLE;
          migrationId = -1;

          return null;
        } else {
          migrationId = findSuccessor(migrationId);
          continue;
        }
      }

      try {
        return node.getRecordFromNode(id);
      } catch (ONodeOfflineException onoe) {
        if (replicaCount < 1) {
          state = NodeState.STABLE;
          migrationId = -1;

          return null;
        } else
          migrationId = findSuccessor(migrationId);
      }
    }
  }

  private Record addData(long id, String data) {
    lockManager.acquireLock(Thread.currentThread(), id, OLockManager.LOCK.EXCLUSIVE);
    try {
      delay();
      // log("Add data for key " + keyId);
      return this.merkleTree.addData(id, data);
    } finally {
      lockManager.releaseLock(Thread.currentThread(), id, OLockManager.LOCK.EXCLUSIVE);
    }
  }

  private void updateData(long id, Record record) {
    lockManager.acquireLock(Thread.currentThread(), id, OLockManager.LOCK.EXCLUSIVE);
    try {
      delay();
      this.merkleTree.updateData(id, record.getShortVersion(), record.getData());
    } finally {
      lockManager.releaseLock(Thread.currentThread(), id, OLockManager.LOCK.EXCLUSIVE);
    }
  }

  private void delay() {
    try {
      Thread.sleep(10);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  private RemoteNodeCallResult<Record> remoteNodeGet(long key, long nodeId, int retryCount) {
    ODHTNode node = nodeLookup.findById(nodeId);
    if (node == null) {
      if (retryCount < MAX_RETRIES) {
        log("Node " + nodeId + " is offline, retry " + retryCount + "-d time.");
        return new RemoteNodeCallResult<Record>(true, null);
      } else {
        log("Node " + nodeId + " is offline, retry limit is reached.");
        throw new ONodeOfflineException("Node " + nodeId + " is offline, retry limit is reached.", null, nodeId);
      }
    }

    try {
      return new RemoteNodeCallResult<Record>(false, node.get(key));
    } catch (ONodeOfflineException e) {
      if (retryCount < MAX_RETRIES) {
        log("Node " + nodeId + " is offline, retry " + retryCount + "-d time.");
        return new RemoteNodeCallResult<Record>(true, null);
      } else {
        log("Node " + nodeId + " is offline, retry limit is reached.");
        throw e;
      }
    }
  }

  private void waitTillJoin() {
    while (state == NodeState.JOIN) {
      log("Wait till node will be joined.");
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
      delay();
      data = db.get(dataId);
    } finally {
      lockManager.releaseLock(Thread.currentThread(), dataId, OLockManager.LOCK.SHARED);
    }
    return data;
  }

  private void putReplica(long id, Record replica) {
    lockManager.acquireLock(Thread.currentThread(), id, OLockManager.LOCK.SHARED);
    try {
      delay();
      this.merkleTree.updateReplica(id, replica);
    } finally {
      lockManager.releaseLock(Thread.currentThread(), id, OLockManager.LOCK.SHARED);
    }
  }

  private void removeData(long id, int version) {
    lockManager.acquireLock(Thread.currentThread(), id, OLockManager.LOCK.EXCLUSIVE);
    try {
      delay();
      merkleTree.deleteData(id, version);
    } finally {
      lockManager.releaseLock(Thread.currentThread(), id, OLockManager.LOCK.EXCLUSIVE);
    }
  }

  public int size() {
    return db.size();
  }

  public NodeState state() {
    return state;
  }

  public void stabilize() {
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

      if (predecessor > -1 && insideInterval(this.id, successorId, predecessor, false)) {
        log("Successor was " + successorId + " is going to be changed to " + predecessor);

        result = fingerPoints.compareAndSet(0, successorId, predecessor);

        if (result)
          log("Successor was successfully changed");
        else
          log("Successor change was failed");

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

      final long prevPredecessor;
      try {
        prevPredecessor = successor.notifyParent(id);
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

      final int successorsSize = (int) Math.ceil(Math.log(nodeLookup.size()) / Math.log(2));

      if (successorsSize > 0) {
        long[] successors;
        try {
          successors = successor.getSuccessors(successorsSize - 1);
        } catch (ONodeOfflineException oof) {
          handleSuccessorOfflineCase(retryCount, successor.getNodeId());

          retryCount++;
          result = false;
          continue;
        }

        if (successors != null) {
          successorsList = successors;
          // log("Successors : " + Arrays.toString(successorsList));
        } else
          log("Returned successors list is empty.");
      }
    }

    // drawRing();
    // log("Stabilization is finished");
  }

  private void handleSuccessorOfflineCase(int retryCount, long successorId) {
    if (retryCount < MAX_RETRIES) {
      log("Successor " + successorId + " is offline will try to find new one and retry. " + retryCount + "-d retry.");

      final long newSuccessorId = findSuccessor(id);
      if (fingerPoints.compareAndSet(0, successorId, newSuccessorId)) {
        final ODHTNode newSuccessorNode = nodeLookup.findById(newSuccessorId);
        if (newSuccessorNode != null)
          try {
            newSuccessorNode.notifyParent(id);
          } catch (ONodeOfflineException noe) {
            fingerPoints.compareAndSet(0, newSuccessorId, successorId);
          }
      }
    } else {
      log("Successor " + successorId + " is offline will try to find new one and retry." + " Max retry count is reached.");
      throw new ONodeOfflineException("Successor " + successorId + " is offline will try to find new one and retry."
          + " Max retry count is reached.", null, successorId);
    }
  }

  public void fixFingers() {
    int nextValue = next.intValue();

    fingerPoints.set(nextValue, findSuccessor((id + 1 << nextValue) & Long.MAX_VALUE));

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

    while (!result) {
      long predecessorId = predecessor.longValue();

      if (predecessorId < 0 || (insideInterval(predecessorId, this.id, nodeId, false))) {
        prevPredecessor = predecessorId;

        result = predecessor.compareAndSet(predecessorId, nodeId);
        if (result)
          log("New predecessor is " + nodeId);
        else {
          log("Predecessor setup was failed.");
          prevPredecessor = -1;
        }

        if (result && predecessorId < 0 && state == NodeState.JOIN) {
          state = NodeState.MERGING;
          log("Status was changed to " + state);
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

  private boolean insideInterval(long from, long to, long value, boolean rightIsIncluded) {
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

  private void log(String message) {
    DateFormat dateFormat = DateFormat.getDateTimeInstance();

    System.out.println(state + " : " + Thread.currentThread().getName() + " : " + id + " : " + dateFormat.format(new Date())
        + " : " + message);
  }

  private void drawRing() {
    try {
      StringBuilder builder = new StringBuilder();

      builder.append("Ring : ");

      builder.append(id);
      ODHTNode node = this;

      Set<Long> processedIds = new HashSet<Long>();
      processedIds.add(id);

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

      log(builder.toString());
    } catch (ONodeOfflineException noe) {
      // ignore
    }
  }

  private final class GlobalMaintenanceProtocol implements Callable<Void> {
    public Void call() throws Exception {
      long idToTest = id;

      mgCycle:
			while (!Thread.currentThread().isInterrupted()) {
        try {
          if (state == null && !state.equals(NodeState.MERGING) && !state.equals(NodeState.STABLE))
            continue;

          long nextId = nextInDB(idToTest);

          long successor = findSuccessor(nextId);
          if (id == successor) {
            idToTest = id;
            continue;
          }

          final ODHTNode successorNode = nodeLookup.findById(successor);
          long[] successors;
          successors = successorNode.getSuccessors(replicaCount);

          for (long s : successors) {
            if (s == id) {
              idToTest = id;
              continue mgCycle;
            }
          }

          List<Long> nodesToReplicate = new ArrayList<Long>();
          nodesToReplicate.add(successor);
          for (long s : successors)
            nodesToReplicate.add(s);

          final Iterator<Record> iterator = db.subMap(idToTest, true, successor, true).values().iterator();

          List<Long> ids = new ArrayList<Long>(100);
          List<ODHTRecordVersion> versions = new ArrayList<ODHTRecordVersion>(100);

          for (long nodeId : nodesToReplicate) {
            while (iterator.hasNext()) {
              final ODHTNode node = nodeLookup.findById(nodeId);
              if (node == null) {
                idToTest = id;
                continue;
              }

              final Record record = iterator.next();

              ids.add(record.getId());
              versions.add(record.getVersion());

              if (ids.size() >= 100) {
                final ODHTRecordVersion[] arrayVersions = new ODHTRecordVersion[versions.size()];
                final long[] arrayIDs = new long[ids.size()];

                for (int i = 0; i < arrayIDs.length; i++)
                  arrayIDs[i] = ids.get(i);

                versions.toArray(arrayVersions);

                final long[] missingIds = node.findMissedRecords(arrayIDs, arrayVersions);

                for (long missingId : missingIds) {
                  final Record replica = db.get(missingId);
                  if (replica != null)
                    node.updateReplica(replica);

                  try {
                    removeData(replica.getId(), replica.getShortVersion());
                  } catch (OConcurrentModificationException e) {
                    // ignore
                  }
                }

                ids.clear();
                versions.clear();
              }
            }
          }

          idToTest = successor;
        } catch (Exception e) {
          log(e.toString());

          idToTest = id;
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

  private static final class RemoteNodeCallResult<T> {
    private boolean repeat;
    private T       data;

    private RemoteNodeCallResult(boolean repeat, T data) {
      this.repeat = repeat;
      this.data = data;
    }
  }
}
