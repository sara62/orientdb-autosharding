package com.orientechnologies.orient.server.distributed;

import com.orientechnologies.common.concur.lock.OLockManager;

import java.text.DateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListMap;
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
	private static final int MAX_RETRIES = 10;

	private AtomicLong predecessor = new AtomicLong(-1);
	private final long id;

	private final AtomicLongArray fingerPoints = new AtomicLongArray(63);

	private final NavigableMap<Long, String> db = new ConcurrentSkipListMap<Long, String>();

	private volatile long migrationId = -1;
	private volatile ODHTNodeLookup nodeLookup;

	private AtomicInteger next = new AtomicInteger(1);
	private final OLockManager<Long, Runnable> lockManager = new OLockManager<Long, Runnable>(true, 500);

	private volatile ExecutorService executorService = Executors.newCachedThreadPool();

	private final Queue<Long> notificationQueue = new ConcurrentLinkedQueue<Long>();

	private volatile long[] successorsList = new long[0];

	private volatile NodeState state;

	private final OMerkleTree merkleTree = new OMerkleTree(db);

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

			if (state != null) {
				executorService.shutdownNow();
				executorService.awaitTermination(10, TimeUnit.MINUTES);

				if (!executorService.isTerminated())
					throw new IllegalStateException("Invalid node state . Not all background processes were terminated.");

				executorService = Executors.newCachedThreadPool();

				db.clear();
			}

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

					final long prevPredecessor = successor.notify(id);
					if (prevPredecessor > -1) {
						final ODHTNode prevPredecessorNode = nodeLookup.findById(prevPredecessor);
						if (prevPredecessorNode != null)
							try {
								prevPredecessorNode.requestStabilization();
							} catch (ONodeOfflineException e) {
								//ignore
							}
					}

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

	public long findSuccessor(long key) {
		while (true) {
//		log("Successor request for key " + key);
			final long successorId = fingerPoints.get(0);

			if (insideInterval(id, successorId, key, true)) {
//			log("Key " + key + " inside interval " + id + " " + successorId);
				return successorId;
			}

			long nodeId = findClosestPrecedingFinger(key);

//		log("Closest preceding node for key " + key + " is " + nodeId);

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
//			log("Successor for key " + key + " is " + successorResult);
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

	private long findClosestPrecedingFinger(long key) {
//		log("Closest preceding request for key " + key);

		for (int i = fingerPoints.length() - 1; i >= 0; i--) {
			final long fingerPoint = fingerPoints.get(i);
			if (fingerPoint > -1 && insideInterval(this.id, key, fingerPoint, false)) {
//				log("Closest preceding finger for key " + key + " is " + fingerPoint);
				return fingerPoint;
			}
		}

//		log("Closest preceding finger for key " + key + " is " + this.id);

		return this.id;
	}

	public long getSuccessor() {
		return fingerPoints.get(0);
	}

	public Long getPredecessor() {
		return predecessor.get();
	}

	public void put(Long key, String data) {
		waitTillJoin();

		int retryCount = 0;

		while (true) {
			final long successorId = findSuccessor(key);
			retryCount++;

			if (successorId == id) {
				if (!checkLocalOwnerShip(key, retryCount))
					continue;

				putData(key, data);

				return;
			} else {
				if (!remoteNodePut(key, data, retryCount, successorId))
					continue;

				return;
			}
		}
	}

	private boolean remoteNodePut(Long key, String data, int retryCount, long nodeId) {
		final ODHTNode node = nodeLookup.findById(nodeId);

		if (node == null) {
			if (retryCount < MAX_RETRIES) {
				log("Node " + nodeId + " is offline, retry " + retryCount + "-d time.");
				return false;
			} else {
				log("Node " + nodeId + " is offline, retry limit is reached.");
				throw new ONodeOfflineException("Node " + nodeId + " is offline, retry limit is reached.",
								null, nodeId);
			}
		}

		try {
			node.put(key, data);
		} catch (ONodeOfflineException ooe) {
			if (retryCount < MAX_RETRIES) {
				log("Node " + nodeId + " is offline, retry " + retryCount + "-d time.");
				return false;
			} else {
				log("Node " + nodeId + " is offline, retry limit is reached.");
				throw new ONodeOfflineException("Node " + nodeId + " is offline, retry limit is reached.",
								null, nodeId);
			}
		}

		return true;
	}

	private void putData(Long keyId, String data) {
		lockManager.acquireLock(Thread.currentThread(), keyId, OLockManager.LOCK.EXCLUSIVE);
		try {
			delay();
//		log("Add data for key " + keyId);
			this.merkleTree.addData(keyId, data);
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

	public String get(Long key, boolean checkOwnerShip) {
		waitTillJoin();
		int retryCount = 0;

		while (true) {
			retryCount++;

			if (checkOwnerShip) {
				final long successorId = findSuccessor(key);
				if (successorId != id) {
					final RemoteNodeCallResult<String> remoteNodeGetResult = remoteNodeGet(key, successorId, retryCount);

					if (remoteNodeGetResult.repeat)
						continue;

					return remoteNodeGetResult.data;
				}

				if (!checkLocalOwnerShip(key, retryCount))
					continue;
			}

			if (state == NodeState.MERGING) {
				String data;
				data = readData(key);

				if (data == null) {
					int migrationRetryCount = 0;

					while (true) {
						ODHTNode migrationNode = nodeLookup.findById(migrationId);

						while (migrationNode == null && migrationRetryCount <= MAX_RETRIES) {

							migrationRetryCount++;

							migrationId = findSuccessor(migrationId);
							if (migrationId == id)
								break;

							migrationNode = nodeLookup.findById(migrationId);
							if (migrationNode != null) {
								try {
									migrationNode.requestMigration(id);
								} catch (ONodeOfflineException noe) {
									migrationNode = null;
								}
							}
						}

						if (migrationNode == null) {
							state = NodeState.STABLE;

							processNotificationQueue();

							return data;
						}

						try {
							data = migrationNode.get(key, false);
						} catch (ONodeOfflineException noe) {
							continue;
						}

						if (data == null)
							return readData(key);
						else
							return data;
					}
				} else
					return data;
			}

			return readData(key);
		}
	}

	private RemoteNodeCallResult<String> remoteNodeGet(long key, long nodeId, int retryCount) {
		ODHTNode node = nodeLookup.findById(nodeId);
		if (node == null) {
			if (retryCount < MAX_RETRIES) {
				log("Node " + nodeId + " is offline, retry " + retryCount + "-d time.");
				return new RemoteNodeCallResult<String>(true, null);
			} else {
				log("Node " + nodeId + " is offline, retry limit is reached.");
				throw new ONodeOfflineException("Node " + nodeId + " is offline, retry limit is reached.",
								null, nodeId);
			}
		}

		try {
			return new RemoteNodeCallResult<String>(false, node.get(key));
		} catch (ONodeOfflineException e) {
			if (retryCount < MAX_RETRIES) {
				log("Node " + nodeId + " is offline, retry " + retryCount + "-d time.");
				return new RemoteNodeCallResult<String>(true, null);
			} else {
				log("Node " + nodeId + " is offline, retry limit is reached.");
				throw e;
			}
		}
	}

	private void processNotificationQueue() {
		Long nodeToNotifyId = notificationQueue.poll();
		while (nodeToNotifyId != null) {
			final ODHTNode node = nodeLookup.findById(nodeToNotifyId);
			if (node != null)
				try {
					node.notifyMigrationEnd(id);
				} catch (ONodeOfflineException noe) {
				}

			nodeToNotifyId = notificationQueue.poll();
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

	public boolean remove(Long key, boolean checkOwnerShip) {
		boolean result = false;

		waitTillJoin();

		//log("Removal request for " + key);

		if (checkOwnerShip) {
			int retryCount = 0;

			while (true) {
				retryCount++;

				final long successorId = findSuccessor(key);
				//log("Successor for key " + key + " is " + successorId);

				if (successorId != id) {
					ODHTNode node = nodeLookup.findById(successorId);
					if (node == null) {
						if (retryCount < MAX_RETRIES) {
							log("Node " + successorId + " is offline, " + retryCount + "-d retry.");
							continue;
						} else {
							log("Node " + successorId + " is offline, retry limit was reached.");
							throw new ONodeOfflineException("Node " + successorId + " is offline, retry limit was reached.",
											null, successorId);
						}
					}

					try {
						return node.remove(key);
					} catch (ONodeOfflineException noe) {
						if (retryCount < MAX_RETRIES) {
							log("Node " + successorId + " is offline, " + retryCount + "-d retry.");
						} else {
							log("Node " + successorId + " is offline, retry limit was reached.");
							throw noe;
						}
					}
				}

				if (checkLocalOwnerShip(key, retryCount))
					break;
			}
		}

		if (state == NodeState.MERGING) {
			int migrationRetryCount = 0;

			while (true) {
				//log("Try to remove key " + key + " from node " + migrationId);

				ODHTNode migrationNode = nodeLookup.findById(migrationId);

				while (migrationNode == null && migrationRetryCount < MAX_RETRIES) {
					log("Node " + migrationId + " is offline . Ask for new one.");

					migrationId = findSuccessor(migrationId);

					log("New node is " + migrationId);

					if (migrationId == id) {
						log("New and current node are the same.");
						break;
					}

					migrationNode = nodeLookup.findById(migrationId);
					if (migrationNode != null) {
						try {
							log("Migration request for " + migrationNode.getNodeId());
							migrationNode.requestMigration(id);
						} catch (ONodeOfflineException noe) {
							migrationNode = null;
							migrationRetryCount++;
						}
					} else
						migrationRetryCount++;
				}

				if (migrationNode == null) {
					log("New migration node was not found. Switch to stable mode.");
					state = NodeState.STABLE;

					processNotificationQueue();

					break;
				}

				try {
					//log("Key " + key + " is going to be removed from migration node.");
					result = migrationNode.remove(key, false);
					//log("Key " + key + " is removed from migration node with result " + result);
					break;
				} catch (ONodeOfflineException noe) {
					//retry
				}
			}
		}

		//log("Key " + key + " is going to be removed from owner node.");
		result = result | removeData(key);
		//log("Key " + key + " is removed from owner node with result " + result);

		return result;
	}

	public void requestMigration(long requesterId) {
		log("Data migration request for node " + requesterId);
		executorService.submit(new MergeCallable(nodeLookup, requesterId));
		log("Data migration was started for node " + requesterId);
	}

	private boolean removeData(Long key) {
		lockManager.acquireLock(Thread.currentThread(), key, OLockManager.LOCK.EXCLUSIVE);
		try {
			delay();
//		log("Remove data for key " + key);
			return merkleTree.deleteData(key);
		} finally {
			lockManager.releaseLock(Thread.currentThread(), key, OLockManager.LOCK.EXCLUSIVE);
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
				prevPredecessor = successor.notify(id);
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
						//ignore
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
//					log("Successors : " + Arrays.toString(successorsList));
				} else
					log("Returned successors list is empty.");
			}
		}

//		drawRing();
//		log("Stabilization is finished");
	}

	private void handleSuccessorOfflineCase(int retryCount, long successorId) {
		if (retryCount < MAX_RETRIES) {
			log("Successor " + successorId + " is offline will try to find new one and retry. " + retryCount +
							"-d retry.");

			final long newSuccessorId = findSuccessor(id);
			if (fingerPoints.compareAndSet(0, successorId, newSuccessorId)) {
				final ODHTNode newSuccessorNode = nodeLookup.findById(newSuccessorId);
				if (newSuccessorNode != null)
					try {
						newSuccessorNode.notify(id);
					} catch (ONodeOfflineException noe) {
						fingerPoints.compareAndSet(0, newSuccessorId, successorId);
					}
			}
		} else {
			log("Successor " + successorId + " is offline will try to find new one and retry." +
							" Max retry count is reached.");
			throw new ONodeOfflineException("Successor " + successorId + " is offline will try to find new one and retry." +
							" Max retry count is reached.", null, successorId);
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

	public long notify(long nodeId) {
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
					int retryCount = 0;

					while (true) {
						migrationId = fingerPoints.get(0);

						final ODHTNode mergeNode = nodeLookup.findById(migrationId);
						if (mergeNode == null) {
							handleSuccessorOfflineCase(retryCount, migrationId);

							retryCount++;
							continue;
						}

						try {
							mergeNode.requestMigration(id);
							break;
						} catch (ONodeOfflineException noe) {
							handleSuccessorOfflineCase(retryCount, migrationId);
							retryCount++;
						}
					}

					state = NodeState.MERGING;
					log("Status was changed to " + state);
				}

				drawRing();
			} else
				result = true;
		}

//		log("Parent check is finished.");

		return prevPredecessor;
	}

	public void notifyMigrationEnd(long nodeId) {
		log("Migration completion notification from " + nodeId);

		waitTillJoin();

		if (nodeId == migrationId) {
			state = NodeState.STABLE;
			log("State was changed to " + state);

			processNotificationQueue();
		}
	}

	public void requestStabilization() {
		stabilize();
	}

	private boolean checkLocalOwnerShip(final long key, final int retryCount) {
		final long predecessorValue = predecessor.get();

		if (predecessorValue > -1 && insideInterval(predecessorValue, id, key, true))
			return true;

		if (retryCount <= MAX_RETRIES) {
			log("Owner for key " + key + " is absent. Predecessor " + predecessorValue + ". " + retryCount +
							" retry.");

			return false;
		}


		log("Owner for key " + key + " is absent. Predecessor " + predecessorValue + ". Retry count is reached.");
		drawRing();

		throw new ODHTKeyOwnerIsAbsentException("Owner for key " + key + " is absent.", key);
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
				if (node == null)
					return;

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
			try {
				keyCycle:
				while (keyIterator.hasNext() && !Thread.currentThread().isInterrupted()) {
					long key = keyIterator.next();

					int retryCount = 0;

					lockManager.acquireLock(Thread.currentThread(), key, OLockManager.LOCK.EXCLUSIVE);
					try {
						while (true) {
							final String data = db.get(key);
							if (data != null) {
								final long nodeId = findSuccessor(key);

								if (nodeId != id) {

									final ODHTNode node = nodeLookup.findById(nodeId);
									if (node == null) {
										if (retryCount < MAX_RETRIES) {
											log("Node with id " + nodeId + " is offline. " + retryCount + "-d retry.");
											retryCount++;

											continue;
										} else {
											log("Node with id " + nodeId + " is offline. Retry limit is reached.");

											continue keyCycle;
										}
									}

									try {
										node.put(key, data);
									} catch (ONodeOfflineException noe) {
										if (retryCount < MAX_RETRIES) {
											log("Node with id " + nodeId + " is offline. " + retryCount + "-d retry.");
											retryCount++;

											continue;
										} else {
											log("Node with id " + nodeId + " is offline. Retry limit is reached.");

											continue keyCycle;
										}
									} catch (ODHTKeyOwnerIsAbsentException oae) {
										if (retryCount < MAX_RETRIES) {
											log("Ring structure is not stabilized . " + retryCount + "-d retry.");
											retryCount++;

											continue;
										} else {
											log("Ring structure is not stabilized. Retry limit is reached.");

											continue keyCycle;
										}
									}

									merkleTree.deleteData(key);
//								log("Remove data for key " + key + " during merge.");
								}
							}
							break;
						}
					} finally {
						lockManager.releaseLock(Thread.currentThread(), key, OLockManager.LOCK.EXCLUSIVE);
					}
				}

				if (state == NodeState.STABLE) {
					final ODHTNode node = nodeLookup.findById(requesterNode);
					if (node != null)
						try {
							node.notifyMigrationEnd(id);
						} catch (ONodeOfflineException noe) {
							//ignore
						}
				} else {
					notificationQueue.add(requesterNode);
					if (state == NodeState.STABLE) {
						processNotificationQueue();
					}
				}

				log("Migration was successfully finished for node " + requesterNode);
				return null;

			} catch (Exception e) {
				log(e.toString());
				throw e;
			}
		}
	}

	private static final class RemoteNodeCallResult<T> {
		private boolean repeat;
		private T data;

		private RemoteNodeCallResult(boolean repeat, T data) {
			this.repeat = repeat;
			this.data = data;
		}
	}
}