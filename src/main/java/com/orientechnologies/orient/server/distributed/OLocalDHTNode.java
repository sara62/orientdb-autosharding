package com.orientechnologies.orient.server.distributed;

import com.orientechnologies.common.concur.lock.OLockManager;

import java.text.DateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
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
	private static final int MAX_RETRIES = 100;

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

					successor.notify(id);

					log("Join completed, successor is " + fingerPoints.get(0));

					return true;
				} catch (ONodeOfflineException ooe) {
					if (ooe.getNodeId() == joinNodeId) {
						log("Node with id " + joinNodeId + " is absent.");
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

	public long findSuccessor(long keyId) {
		while (true) {
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
			if (node == null) {
				if (nodeId != successorId) {
					clearFingerPoint(nodeId);
					continue;
				} else {
					final long[] successors = successorsList;
					for (final long successor : successors) {
						final ODHTNode successorNode = nodeLookup.findById(successor);
						if (successorNode != null) {
							try {
								return successorNode.findSuccessor(keyId);
							} catch (ONodeOfflineException noe) {
								log(noe.toString());
							}
						}
					}
					throw new ONodeOfflineException("Node " + nodeId + " is offline .", null, nodeId);
				}
			}

			try {
				return node.findSuccessor(keyId);
			} catch (ONodeOfflineException e) {
				if (nodeId != successorId) {
					clearFingerPoint(nodeId);
				} else {
					final long[] successors = successorsList;
					for (final long successor : successors) {
						final ODHTNode successorNode = nodeLookup.findById(successor);
						if (successorNode != null) {
							try {
								return successorNode.findSuccessor(keyId);
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
		fingerPointCycle:
		while (true) {
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
			return new long[]{
							fingerPoints.get(0)
			};

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

		int retryCount = 0;
		while (true) {
//		log("Data is going to be added with key " + dataId);
			final long successorId = findSuccessor(dataId);
			if (successorId == id) {
//			log("Owner for key " + dataId + " is the same as requested node.");
				putData(dataId, data);
				return;
			} else {
//			log("Owner for key " + dataId + " is " + successorId);
				final ODHTNode node = nodeLookup.findById(successorId);

				if (node == null) {
					if (retryCount < MAX_RETRIES) {
						retryCount++;
						log("Node " + successorId + " is offline, retry " + retryCount + "-d time.");
						continue;
					} else {
						log("Node " + successorId + " is offline, retry limit is reached.");
						throw new ONodeOfflineException("Node " + successorId + " is offline, retry limit is reached.",
										null, successorId);
					}
				}

				try {
					node.put(dataId, data);
				} catch (ONodeOfflineException ooe) {
					if (retryCount < MAX_RETRIES) {
						retryCount++;
						log("Node " + successorId + " is offline, retry " + retryCount + "-d time.");
						continue;
					} else {
						log("Node " + successorId + " is offline, retry limit is reached.");
						throw new ONodeOfflineException("Node " + successorId + " is offline, retry limit is reached.",
										null, successorId);
					}
				}

				return;
			}
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
		int retryCount = 0;

		while (true) {
//		log("Data with key " + dataId +  " were requested.");
			if (checkOwnerShip) {
				final long successorId = findSuccessor(dataId);
				if (successorId != id) {
//				log("Successor for " + dataId + " is " + successorId);

					ODHTNode node = nodeLookup.findById(successorId);
					if (node == null) {
						if (retryCount < MAX_RETRIES) {
							retryCount++;
							log("Node " + successorId + " is offline, retry " + retryCount + "-d time.");
							continue;
						} else {
							log("Node " + successorId + " is offline, retry limit is reached.");
							throw new ONodeOfflineException("Node " + successorId + " is offline, retry limit is reached.",
											null, successorId);
						}
					}

					try {
						return node.get(dataId);
					} catch (ONodeOfflineException e) {
						if (retryCount < MAX_RETRIES) {
							retryCount++;
							log("Node " + successorId + " is offline, retry " + retryCount + "-d time.");
							continue;
						} else {
							log("Node " + successorId + " is offline, retry limit is reached.");
							throw new ONodeOfflineException("Node " + successorId + " is offline, retry limit is reached.",
											null, successorId);
						}
					}
				}
			}

//		log("Successor match for key " + dataId);

			if (state == NodeState.MERGING) {
				String data;
				data = readData(dataId);

				if (data == null) {
					ODHTNode migrationNode = nodeLookup.findById(migrationId);
					int migrationRetryCount = 0;

					while (migrationNode == null && migrationRetryCount < MAX_RETRIES) {
						migrationId = findSuccessor(migrationId);
						migrationNode = nodeLookup.findById(migrationId);
						if (migrationNode != null) {
							try {
								migrationNode.requestMigration(id);
							} catch (ONodeOfflineException noe) {
								migrationNode = null;
								migrationRetryCount++;
							}
						} else
							migrationRetryCount++;
					}

					if (migrationNode == null) {
						state = NodeState.STABLE;

						Long nodeToNotifyId = notificationQueue.poll();
						while (nodeToNotifyId != null) {
							final ODHTNode node = nodeLookup.findById(nodeToNotifyId);
							try {
								node.notifyMigrationEnd(id);
							} catch (ONodeOfflineException noe) {
							}
							nodeToNotifyId = notificationQueue.poll();
						}

						return data;
					}

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
			//TODO:migration node is down what to do ?
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

			if (successor == null) {
				log("Successor " + successorId + " is offline will try to find new one and retry.");
				fingerPoints.compareAndSet(0, successorId, findSuccessor(id));
				result = false;
				continue;
			}

			Long predecessor;

			try {
				predecessor = successor.getPredecessor();
			} catch (ONodeOfflineException ooe) {
				log("Successor " + successorId + " is offline will try to find new one and retry.");
				fingerPoints.compareAndSet(0, successorId, findSuccessor(id));
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
						log("Successor " + predecessor + " is offline will try to find new one and retry.");
						fingerPoints.compareAndSet(0, predecessor, findSuccessor(id));
						result = false;
						continue;
					}
				}

				drawRing();
			} else
				result = true;

			if (successor.getNodeId() != id)
				try {
					successor.notify(id);
				} catch (ONodeOfflineException ooe) {
					log("Successor " + successor.getNodeId() + " is offline will try to find new one and retry.");
					fingerPoints.compareAndSet(0, predecessor, findSuccessor(id));
					result = false;
					continue;
				}


			final int successorsSize = (int) Math.ceil(Math.log(nodeLookup.size()) / Math.log(2)) - 1;
//		log("Successors size " + successorsSize);

			if (successorsSize > 0) {
				long[] successors;
				try {
					successors = successor.getSuccessors(successorsSize - 1);
				} catch (ONodeOfflineException oof) {
					log("Successor " + successor.getNodeId() + " is offline will try to find new one and retry.");
					fingerPoints.compareAndSet(0, successor.getNodeId(), findSuccessor(id));
					result = false;
					continue;
				}

				if (successors != null) {
					successorsList = successors;
//					log("Successors : " + Arrays.toString(successorsList));
				} else
					log("Returned successors list is empty.");
			}
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
					//TODO:migration node is down what to do
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
				//TODO:migration node is down what to do
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
				successor = node.getSuccessor();
			}

			builder.append(".");

			log(builder.toString());
		} catch (ONodeOfflineException noe) {
			//ignore
		}
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