package com.orientechnologies.orient.server.distributed;

import java.text.DateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
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

	private final Map<Long, String> data = new ConcurrentHashMap<Long, String>();

	private volatile ODHTNodeLookup nodeLookup;
	private AtomicInteger next = new AtomicInteger(1);

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
		long nodeId;
		ODHTNode node;

		//log("Successor request for key id " + keyId);
		while (true) {
			try {
				do {
					nodeId = findPredecessor(keyId);

					node = nodeLookup.findById(nodeId);
				} while (node == null);

		//	log("Successor for " + keyId + " is " + node.getSuccessor());
				return node.getSuccessor();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public long findPredecessor(long keyId) {
		//log("Predecessor request for key id " + keyId);

		while (true) {
			ODHTNode node = this;

			try {
				while (!insideInterval(node.getNodeId(), node.getSuccessor(), keyId, true)) {
					final long nodeId = node.findClosestPrecedingFinger(keyId);
					final ODHTNode foundNode = nodeLookup.findById(nodeId);
					if (foundNode != null)
						node = foundNode;
				}

			//log("Predecessor for " + keyId + " is " + node.getNodeId());
				return node.getNodeId();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public long findClosestPrecedingFinger(long keyId) {
		//log("Closest finger request for key id " + keyId);

		for (int i = fingerPoints.length() - 1; i >= 0; i--) {
			final long fingerPoint = fingerPoints.get(i);
			if (fingerPoint > -1 && insideInterval(this.id, keyId, fingerPoint, false)) {
		//	log("Closest finger for " + keyId + " is " + fingerPoint);
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
		this.data.put(dataId, data);
	}

	public String get(Long dataId) {
		return data.get(dataId);
	}

	public int size() {
		return data.size();
	}

	public void stabilize() {
		log("Stabilization is started");
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
					successor = nodeLookup.findById(predecessor);

				if (result)
					log("Successor was successfully changed");
				else
					log("Successor change was failed");
			} else
				result = true;
		}

		if (successor.getNodeId() != id)
			successor.notify(id);

		drawRing();
		log("Stabilization is finished");
	}

	public void fixFingers() {
		int nextValue = next.intValue();

		log("Fix of fingers is started for interval " + ((id + 1 << nextValue) & Long.MAX_VALUE));

		fingerPoints.set(nextValue, findSuccessor((id + 1 << nextValue) & Long.MAX_VALUE));

		next.compareAndSet(nextValue, nextValue + 1);

		while (next.intValue() > 62) {
			nextValue = next.intValue();
			if (nextValue > 62)
				next.compareAndSet(nextValue, 1);

			log("Next value is changed to 1");
		}

		log("Fix of fingers was finished.");
	}

	public void fixPredecessor() {
		log("Fix of predecessor is started");

		boolean result = false;

		while (!result) {
			long predecessorId = predecessor.longValue();

			if (predecessorId > -1 && nodeLookup.findById(predecessorId) == null) {
				result = predecessor.compareAndSet(predecessorId, -1);

				log("Predecessor " + predecessorId + " left the cluster");
			}
			else
				result = true;
		}

		log("Fix of predecessor is finished");
	}

	public void notify(long nodeId) {
		log("Node " + nodeId + " thinks it can be our parent");

		boolean result = false;

		while (!result) {
			long predecessorId = predecessor.longValue();

			if (predecessorId < 0 || (insideInterval(predecessorId, this.id, nodeId, false))) {
				result = predecessor.compareAndSet(predecessorId, nodeId);
				if (result)
					log("New predecessor is " + nodeId);
				else
					log("Predecessor setup was failed.");

				drawRing();
			}
			else
				result = true;
		}

		log("Parent check is finished.");

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

		System.out.println(Thread.currentThread().getName() + " : " + id + " : " +
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
}