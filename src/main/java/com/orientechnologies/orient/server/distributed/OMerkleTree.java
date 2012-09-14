package com.orientechnologies.orient.server.distributed;

import java.util.NavigableMap;

/**
 * @author Andrey Lomakin
 * @since 13.09.12
 */
public class OMerkleTree {
	private final OMerkleTreeNode[] roots;

	public OMerkleTree(final NavigableMap<Long, String> db) {
		final OMerkleTreeNode[] newRoots = new OMerkleTreeNode[64];

		for (int i = 0; i < 64; i++)
			newRoots[i] = new OMerkleTreeNode(db);

		this.roots = newRoots;
	}

	public void addData(final long key, final String data) {
		final int childIndex = OMerkleTreeNode.childIndex(0, key);
		final long startKey = OMerkleTreeNode.startNodeKey(1, childIndex, 0);

		final OMerkleTreeNode node = roots[childIndex];
		node.addData(1, startKey, key, data);
	}

	public boolean deleteData(final long key) {
		final int childIndex = OMerkleTreeNode.childIndex(0, key);
		final long startKey = OMerkleTreeNode.startNodeKey(1, childIndex, 0);

		final OMerkleTreeNode node = roots[childIndex];
		return node.deleteData(1, startKey, key);
	}

	public OMerkleTreeNode getRoot(int index) {
		return roots[index];
	}

	public OMerkleTreeNode getRootForKey(long key) {
		final int childIndex = OMerkleTreeNode.childIndex(0, key);
		return roots[childIndex];
	}

}