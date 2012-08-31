package com.orientechnologies.orient.server.distributed;

import com.orientechnologies.common.concur.lock.OLockManager;

import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

/**
 * @author Andrey Lomakin
 * @since 17.08.12
 */
public class OLocalDHTNode implements ODHTNode {
	private AtomicLong predecessor = new AtomicLong(-1);

	private final long id;
	private final AtomicLongArray fingerPoints = new AtomicLongArray(63);

	private final Map<Long, String> db = new ConcurrentHashMap<Long, String>();

	private volatile long migrationId = -1;
	private volatile ODHTNodeLookup nodeLookup;
	private AtomicInteger next = new AtomicInteger(1);
	private final OLockManager<Long, Runnable> lockManager = new OLockManager<Long, Runnable>(true, 500);

	private final ExecutorService executorService = Executors.newCachedThreadPool();
	private final Queue<Long> notificationQueue = new ConcurrentLinkedQueue<Long>();
	private volatile long[] successorsList = new long[0];

	private NodeState state;

	public OLocalDHTNode(long id) {
		this.id = id;
		for (int i = 0; i < fingerPoints.length(); i++)
			fingerPoints.set(i, -1);
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

	public boolean join(long joinNodeId) {
		try {
			log("Join is started using node with id " + joinNodeId);

			final ODHTNode node = nodeLookup.findById(joinNodeId);
			if (node == null) {
				log("Node with id " + joinNodeId + " is absent.");
				return false;
			}

			state = NodeState.JOIN;

			executorService.shutdownNow();
			executorService.awaitTermination(10, TimeUnit.MINUTES);

			if (!executorService.isTerminated())
				throw new IllegalStateException("Invalid node state . Not all background processes were terminated.");

			db.clear();

			predecessor.set(-1);
			fingerPoints.set(0, node.findSuccessor(id));

			log("Join completed, successor is " + fingerPoints.get(0));

			ODHTNode successor = nodeLookup.findById(fingerPoints.get(0));
			if (successor == null) {
				log("Node with id " + fingerPoints.get(0) + " is absent .");
				return false;
			}

			successor.notify(id);

			log("Join was finished");

			return true;
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
	}

	public long findSuccessor(long keyId) {
		final long successorId = fingerPoints.get(0);
		//log("Find successor for key " + keyId + ", successor is " + successorId);

		if (insideInterval(id, successorId, keyId, true)) {
//			log("Successor is " + successorId);
			return successorId;
		}

//		log("Find closest finger point for key " + keyId);
		long nodeId = findClosestPrecedingFinger(keyId);
//		log("Closest fingerpoint is " + nodeId);

		ODHTNode node = nodeLookup.findById(nodeId);

		return node.findSuccessor(keyId);
	}

	public long[] getSuccessors(int depth) {
		if (depth == 0)
			return new long[]{
							fingerPoints.get(0)
			};

		ODHTNode node = nodeLookup.findById(fingerPoints.get(0));

		long[] successors = new long[depth + 1];
		long[] result = node.getSuccessors(depth - 1);

		System.arraycopy(result, 0, successors, 1, result.length);
		successors[0] = fingerPoints.get(0);

		return successors;
	}

	private long findClosestPrecedingFinger(long keyId) {
		for (int i = fingerPoints.length() - 1; i >= 0; i--) {
			final long fingerPoint = fingerPoints.get(i);
			if (fingerPoint > -1 && insideInterval(this.id, keyId, fingerPoint, false)) {
				return fingerPoint;
			}
		}

		return this.id;
	}

	public long getSuccessor() {
		return fingerPoints.get(0);
	}

	public Long getPredecessor() {
		return predecessor.get();
	}

	public void put(Long dataId, String data) {
		waitTillJoin();

//		log("Data is going to be added with key " + dataId);
		final long successorId = findSuccessor(dataId);
		if (successorId == id) {
//			log("Owner for key " + dataId + " is the same as requested node.");
			putData(dataId, data);
		} else {
//			log("Owner for key " + dataId + " is " + successorId);
			final ODHTNode node = nodeLookup.findById(successorId);
			node.put(dataId, data);
		}
	}

	private void putData(Long keyId, String data) {
//	log("Put for " + dataId);
		lockManager.acquireLock(Thread.currentThread(), keyId, OLockManager.LOCK.EXCLUSIVE);
		try {
			delay();
			this.db.put(keyId, data);
		} finally {
			lockManager.releaseLock(Thread.currentThread(), keyId, OLockManager.LOCK.EXCLUSIVE);
		}
	}

	private void delay() {
		try {
			Thread.sleep(10);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public String get(Long dataId) {
		return get(dataId, true);
	}

	public String get(Long dataId, boolean checkOwnerShip) {
		waitTillJoin();

//		log("Data with key " + dataId +  " were requested.");
		if (checkOwnerShip) {
			final long successorId = findSuccessor(dataId);
			if (successorId != id) {
//				log("Successor for " + dataId + " is " + successorId);

				ODHTNode node = nodeLookup.findById(successorId);
				return node.get(dataId);
			}
		}

//		log("Successor match for key " + dataId);

		if (state == NodeState.MERGING) {
			String data;
			data = readData(dataId);

			if (data == null) {
				final ODHTNode migrationNode = nodeLookup.findById(migrationId);
				data = migrationNode.get(dataId, false);

				if (data == null && migrationNode.getNodeId() != id)
					return readData(dataId);
				else
					return data;
			} else
				return data;
		}

		return readData(dataId);
	}

	private void waitTillJoin() {
		while (state == NodeState.JOIN) {
			log("Wait till node will be joined.");
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	private String readData(Long dataId) {
		String data;
		lockManager.acquireLock(Thread.currentThread(), dataId, OLockManager.LOCK.SHARED);
		try {
			delay();
			data = db.get(dataId);
		} finally {
			lockManager.releaseLock(Thread.currentThread(), dataId, OLockManager.LOCK.SHARED);
		}
		return data;
	}

	public boolean remove(Long keyId) {
		return remove(keyId, true);
	}

	public boolean remove(Long keyId, boolean checkOwnerShip) {
		boolean result = false;

		waitTillJoin();

//		log("Request to remove key " + keyId);

		if (checkOwnerShip) {
			final long successorId = findSuccessor(keyId);
			if (successorId != id) {
//  			log("Successor for " + keyId + " is " + successorId);

				ODHTNode node = nodeLookup.findById(successorId);
				return node.remove(keyId);
			}
		}

		if (state == NodeState.MERGING) {
			final ODHTNode successorNode = nodeLookup.findById(migrationId);
			result = successorNode.remove(keyId, false);
		}

		result = result | removeData(keyId);

		return result;
	}

	public void requestMigration(long requesterId) {
		log("Data migration request for node " + requesterId);
		executorService.submit(new MergeCallable(nodeLookup, requesterId));
		log("Data migration was started for node " + requesterId);
	}

	private boolean removeData(Long dataId) {
		lockManager.acquireLock(Thread.currentThread(), dataId, OLockManager.LOCK.EXCLUSIVE);
		try {
			delay();
			return db.remove(dataId) != null;
		} finally {
			lockManager.releaseLock(Thread.currentThread(), dataId, OLockManager.LOCK.EXCLUSIVE);
		}
	}

	public int size() {
		return db.size();
	}

	public NodeState state() {
		return state;
	}

	public void stabilize() {
		//log("Stabilization is started");
		boolean result = false;

		ODHTNode successor = null;

		while (!result) {
			long successorId = fingerPoints.get(0);

			successor = nodeLookup.findById(successorId);

			Long predecessor = successor.getPredecessor();

			if (predecessor > -1 && insideInterval(this.id, successorId, predecessor, false)) {
				log("Successor was " + successorId + " is going to be changed to " + predecessor);

				result = fingerPoints.compareAndSet(0, successorId, predecessor);

				if (result)
					log("Successor was successfully changed");
				else
					log("Successor change was failed");

				if (result)
					successor = nodeLookup.findById(predecessor);

				drawRing();
			} else
				result = true;
		}

		if (successor.getNodeId() != id)
			successor.notify(id);

		final int successorsSize = (int) Math.ceil(Math.log(nodeLookup.size()) / Math.log(2)) - 1;
		log("Successors size " + successorsSize);

		if (successorsSize > 0) {
			final ODHTNode node = nodeLookup.findById(fingerPoints.get(0));
			successorsList = node.getSuccessors(successorsSize - 1);

			log("Successors : " +  Arrays.toString(successorsList));
		}

//		drawRing();
//		log("Stabilization is finished");
	}

	public void fixFingers() {
		int nextValue = next.intValue();

//	log("Fix of fingers is started for interval " + ((id + 1 << nextValue) & Long.MAX_VALUE));

		fingerPoints.set(nextValue, findSuccessor((id + 1 << nextValue) & Long.MAX_VALUE));

		next.compareAndSet(nextValue, nextValue + 1);

		while (next.intValue() > 62) {
			nextValue = next.intValue();
			if (nextValue > 62)
				next.compareAndSet(nextValue, 1);

//			log("Next value is changed to 1");
		}

//		log("Fix of fingers was finished.");
	}

	public void fixPredecessor() {
//		log("Fix of predecessor is started");

		boolean result = false;

		while (!result) {
			long predecessorId = predecessor.longValue();

			if (predecessorId > -1 && nodeLookup.findById(predecessorId) == null) {
				result = predecessor.compareAndSet(predecessorId, -1);

//				log("Predecessor " + predecessorId + " left the cluster");
			} else
				result = true;
		}

//		log("Fix of predecessor is finished");
	}

	public void notify(long nodeId) {
//		log("Node " + nodeId + " thinks it can be our parent");

		boolean result = false;

		while (!result) {
			long predecessorId = predecessor.longValue();

			if (predecessorId < 0 || (insideInterval(predecessorId, this.id, nodeId, false))) {
				result = predecessor.compareAndSet(predecessorId, nodeId);
				if (result)
					log("New predecessor is " + nodeId);
				else
					log("Predecessor setup was failed.");

				if (result && predecessorId < 0 && state == NodeState.JOIN) {

					migrationId = fingerPoints.get(0);
					final ODHTNode mergeNode = nodeLookup.findById(migrationId);
					mergeNode.requestMigration(id);

					state = NodeState.MERGING;
					log("Status was changed to " + state);
				}

				drawRing();
			} else
				result = true;
		}

//		log("Parent check is finished.");

	}

	public void notifyMigrationEnd(long nodeId) {
		log("Migration completion notification from " + nodeId);

		waitTillJoin();

		if (nodeId == migrationId) {
			state = NodeState.STABLE;
			log("State was changed to " + state);

			Long nodeToNotifyId = notificationQueue.poll();
			while (nodeToNotifyId != null) {
				final ODHTNode node = nodeLookup.findById(nodeToNotifyId);
				node.notifyMigrationEnd(id);
				nodeToNotifyId = notificationQueue.poll();
			}
		}
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

		System.out.println(state + " : " + Thread.currentThread().getName() + " : " + id + " : " +
						dateFormat.format(new Date()) + " : " + message);
	}

	private void drawRing() {
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
			successor = node.getSuccessor();
		}

		builder.append(".");

		log(builder.toString());
	}


	private final class MergeCallable implements Callable<Void> {
		private Iterator<Long> keyIterator;
		private final ODHTNodeLookup nodeLookup;
		private final long requesterNode;

		private MergeCallable(ODHTNodeLookup nodeLookup, long requesterNode) {
			this.nodeLookup = nodeLookup;
			this.requesterNode = requesterNode;
			this.keyIterator = db.keySet().iterator();
		}

		public Void call() throws Exception {
			while (keyIterator.hasNext() && !Thread.currentThread().isInterrupted()) {
				long key = keyIterator.next();
//				log("Migration - examine data with key : " + key);
				lockManager.acquireLock(Thread.currentThread(), key, OLockManager.LOCK.EXCLUSIVE);
				try {
					final String data = db.get(key);
					if (data != null) {
						final long nodeId = findSuccessor(key);
						if (nodeId != id) {
//							log("Key " + key + " belongs to node " + nodeId + ". Key is going to be migrated.");
							final ODHTNode node = nodeLookup.findById(nodeId);
							node.put(key, data);

							keyIterator.remove();
//							log("Key " + key + " was successfully removed.");
						}
					}
//					else {
//						log("Key " + key + " is kept on current node.");
//					}
				} finally {
					lockManager.releaseLock(Thread.currentThread(), key, OLockManager.LOCK.EXCLUSIVE);
				}
			}

			if (state == NodeState.STABLE) {
				final ODHTNode node = nodeLookup.findById(requesterNode);
				node.notifyMigrationEnd(id);
			} else {
				notificationQueue.add(requesterNode);
				if (state == NodeState.STABLE) {
					Long nodeToNotifyId = notificationQueue.poll();
					while (nodeToNotifyId != null) {
						final ODHTNode node = nodeLookup.findById(nodeToNotifyId);
						node.notifyMigrationEnd(id);
						nodeToNotifyId = notificationQueue.poll();
					}
				}
			}

			log("Migration was successfully finished for node " + requesterNode);
			return null;
		}
	}
}