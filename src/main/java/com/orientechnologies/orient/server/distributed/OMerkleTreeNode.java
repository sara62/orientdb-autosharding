package com.orientechnologies.orient.server.distributed;

import com.orientechnologies.common.concur.resource.OSharedResourceAdaptive;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.TreeSet;

/**
 * @author Andrey Lomakin
 * @since 04.09.12
 */
public class OMerkleTreeNode extends OSharedResourceAdaptive {
	private final NavigableMap<Long, String> db;

	private int count;
	private OMerkleTreeNode[] children;

	private byte[] hash;

	public OMerkleTreeNode(final NavigableMap<Long, String> db) {
		count = 0;
		children = null;

		MessageDigest sha = sha();
		hash = sha.digest();

		this.db = db;
	}

	public OMerkleTreeNode(int count, byte[] hash, NavigableMap<Long, String> db) {
		this.db = db;
		this.count = count;
		this.hash = hash;
	}

	public void addData(int level, long offset, long key, String data) {
		OMerkleTreeNode treeNode = this;
		final List<PathItem> hashPathNodes = new ArrayList<PathItem>();

		treeNode.acquireExclusiveLock();

		int childIndex = 0;

		while (!treeNode.isLeaf()) {
			hashPathNodes.add(new PathItem(treeNode, childIndex, offset));

			offset = startNodeKey(level, childIndex, offset);

			childIndex = childIndex(level, key);

			final OMerkleTreeNode child = treeNode.getChild(childIndex);
			child.acquireExclusiveLock();
			treeNode.releaseExclusiveLock();

			treeNode = child;

			level++;
		}

		db.put(key, data);

		treeNode.count++;

		if (treeNode.getKeyCount() <= 64)
			rehashLeafNode(level, offset, treeNode, childIndex);
		else {
			final long startKey = startNodeKey(level, childIndex, offset);

			final Iterator<Long> keyIterator = db.tailMap(startKey, true).keySet().iterator();
			final NavigableSet<Long> keysToHash = new TreeSet<Long>();

			final int keyCount = treeNode.getKeyCount();
			for (int i = 0; i < keyCount; i++) {
				final long currentKey = keyIterator.next();
				keysToHash.add(currentKey);
			}

			convertToInternalNode(level, startKey, keysToHash, treeNode);
			hashPathNodes.add(new PathItem(treeNode, childIndex, offset));
		}

		treeNode.releaseExclusiveLock();

		rehashParentNodes(hashPathNodes);
	}

	private void rehashLeafNode(int level, long offset, OMerkleTreeNode treeNode, int childIndex) {
		final MessageDigest messageDigest = sha();
		final ByteBuffer byteBuffer = ByteBuffer.allocate(treeNode.getKeyCount() * 8);

		final long startKey = startNodeKey(level, childIndex, offset);

		final Iterator<Long> keyIterator = db.tailMap(startKey, true).keySet().iterator();

		final int keyCount = treeNode.getKeyCount();
		for (int i = 0; i < keyCount; i++) {
			final long currentKey = keyIterator.next();
			byteBuffer.putLong(currentKey);
		}

		byteBuffer.rewind();
		messageDigest.update(byteBuffer);

		treeNode.hash = messageDigest.digest();
	}

	public void deleteData(int level, long offset, long key) {
		OMerkleTreeNode treeNode = this;
		final List<PathItem> hashPathNodes = new ArrayList<PathItem>();

		treeNode.acquireExclusiveLock();

		int childIndex = 0;

		while (!treeNode.isLeaf()) {
			hashPathNodes.add(new PathItem(treeNode, childIndex, offset));

			offset = startNodeKey(level, childIndex, offset);

			childIndex = childIndex(level, key);

			final OMerkleTreeNode child = treeNode.getChild(childIndex);
			child.acquireExclusiveLock();
			treeNode.releaseExclusiveLock();

			treeNode = child;

			level++;
		}

		db.remove(key);

		treeNode.count--;

		rehashLeafNode(level, offset, treeNode, childIndex);

		treeNode.releaseExclusiveLock();

		rehashParentNodes(hashPathNodes);
	}

	public static long startNodeKey(int level, long index, long offset) {
		return (1L << (64 - 6 * level)) * index + offset;
	}

	public static int childIndex(int level, long key) {
		return (int) ((key >> (64 - 6 * (level + 1))) & 63L);
	}

	private void convertToInternalNode(int level, long offset, NavigableSet<Long> keys, OMerkleTreeNode treeNode) {
		final OMerkleTreeNode[] children = new OMerkleTreeNode[64];
		final ByteBuffer parentBuffer = ByteBuffer.allocate(64 * 20);

		for (int i = 0; i < 64; i++) {
			final int childLevel = level + 1;
			final long startChildKey = startNodeKey(childLevel, i, offset);
			final long endChildKey = startNodeKey(childLevel, i + 1, offset);

			final NavigableSet<Long> childKeysToAdd;

			if (endChildKey > startChildKey)
				childKeysToAdd = keys.subSet(startChildKey, true, endChildKey, false);
			else
				childKeysToAdd = keys.tailSet(startChildKey, true);

			final int childSize = childKeysToAdd.size();
			final OMerkleTreeNode child;

			if (childSize <= 64) {
				final ByteBuffer buffer = ByteBuffer.allocate(childSize * 8);
				for (long currentKey : childKeysToAdd)
					buffer.putLong(currentKey);

				buffer.rewind();

				final MessageDigest sha = sha();
				sha.update(buffer);

				child = new OMerkleTreeNode(childSize, sha.digest(), db);
			} else {
				child = new OMerkleTreeNode(db);
				convertToInternalNode(childLevel, startChildKey, childKeysToAdd, child);
			}

			children[i] = child;
			parentBuffer.put(child.getHash());
		}

		parentBuffer.rewind();

		final MessageDigest sha = sha();
		sha.update(parentBuffer);

		treeNode.children = children;
		treeNode.count = 0;
		treeNode.hash = sha.digest();
	}

	public int getKeyCount() {
		acquireSharedLock();
		try {
			return count;
		} finally {
			releaseSharedLock();
		}
	}

	public boolean isLeaf() {
		acquireSharedLock();
		try {
			return children == null;
		} finally {
			releaseSharedLock();
		}
	}

	public OMerkleTreeNode getChild(int index) {
		acquireSharedLock();
		try {
			if (children == null)
				return null;

			return children[index];
		} finally {
			releaseSharedLock();
		}
	}

	public byte[] getHash() {
		acquireSharedLock();
		try {
			return hash;
		} finally {
			releaseSharedLock();
		}
	}

	private void rehashParentNodes(final List<PathItem> path) {
		for (int level = path.size(); level >= 1; level--) {
			final PathItem pathItem = path.get(level - 1);
			final OMerkleTreeNode node = pathItem.node;

			node.acquireExclusiveLock();
			if (node.children == null)
				node.releaseExclusiveLock();
			else {
				final ByteBuffer byteBuffer = ByteBuffer.allocate(20 * 64);

				int childrenCount = 0;

				List<OMerkleTreeNode> lockedNodes = new ArrayList<OMerkleTreeNode>(64);

				for (int i = 0; i < 64; i++) {
					final OMerkleTreeNode child = node.children[i];
					child.acquireSharedLock();

					lockedNodes.add(child);

					byteBuffer.put(child.getHash());

					if (child.isLeaf())
						childrenCount += child.getKeyCount();
					else
						childrenCount = 65;
				}


				if (childrenCount <= 64) {
					byteBuffer.clear();

					node.children = null;
					node.count = childrenCount;

					rehashLeafNode(level, pathItem.offset, node, pathItem.childIndex);
				} else {
					final MessageDigest sha = sha();

					byteBuffer.rewind();
					sha.update(byteBuffer);

					node.hash = sha.digest();
				}

				node.releaseExclusiveLock();

				for (OMerkleTreeNode treeNode : lockedNodes)
					treeNode.releaseSharedLock();
			}
		}
	}

	private MessageDigest sha() {
		try {
			return MessageDigest.getInstance("SHA-1");
		} catch (NoSuchAlgorithmException nsae) {
			throw new IllegalStateException(nsae);
		}
	}

	private static final class PathItem {
		private OMerkleTreeNode node;
		private int childIndex;
		private long offset;

		private PathItem(OMerkleTreeNode node, int childIndex, long offset) {
			this.node = node;
			this.childIndex = childIndex;
			this.offset = offset;
		}
	}
}