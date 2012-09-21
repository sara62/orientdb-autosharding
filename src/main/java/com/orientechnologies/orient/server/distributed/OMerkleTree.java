package com.orientechnologies.orient.server.distributed;

import java.util.NavigableMap;

/**
 * @author Andrey Lomakin
 * @since 13.09.12
 */
public class OMerkleTree {
	private final OMerkleTreeNode[] roots;

	public OMerkleTree(final NavigableMap<Long, Record> db) {
		final OMerkleTreeNode[] newRoots = new OMerkleTreeNode[64];

		for (int i = 0; i < 64; i++)
			newRoots[i] = new OMerkleTreeNode(db);

		this.roots = newRoots;
	}

	public long addData(final long id, final String data) {
		final int childIndex = OMerkleTreeNode.childIndex(0, id);
		final long startKey = OMerkleTreeNode.startNodeId(1, childIndex, 0);

		final OMerkleTreeNode node = roots[childIndex];

  //System.out.println("Add data: key " + key + " index " + childIndex + " start key " + startKey);
		return node.addData(1, startKey, id, data);
	}

	public void deleteData(final long id, final long version) {
		final int childIndex = OMerkleTreeNode.childIndex(0, id);
		final long startId = OMerkleTreeNode.startNodeId(1, childIndex, 0);

		final OMerkleTreeNode node = roots[childIndex];
  //System.out.println("Delete data: key " + key + " index " + childIndex + " start key " + startKey);
		node.deleteData(1, startId, id, version);
	}

	public OMerkleTreeNode getRoot(int index) {
		return roots[index];
	}

	public OMerkleTreeNode getRootForKey(long key) {
		final int childIndex = OMerkleTreeNode.childIndex(0, key);
		return roots[childIndex];
	}

}