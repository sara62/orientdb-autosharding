package com.orientechnologies.orient.server.distributed;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;

import com.orientechnologies.orient.core.id.OAutoShardedRecordId;

/**
 * @author Andrey Lomakin
 * @since 13.09.12
 */
public class OMerkleTree {
  private final int                                        clusterId;
  private final OMerkleTreeNode[]                          roots;
  private final NavigableMap<OAutoShardedRecordId, Record> db;

  public OMerkleTree(final NavigableMap<OAutoShardedRecordId, Record> db, final int clusterId) {
    this.db = db;
    this.clusterId = clusterId;

    final OMerkleTreeNode[] newRoots = new OMerkleTreeNode[64];

    for (int i = 0; i < 64; i++)
      newRoots[i] = new OMerkleTreeNode(db, clusterId);

    this.roots = newRoots;
  }

  public Record addData(final OAutoShardedRecordId id, final String data) {
    final int childIndex = OMerkleTreeNode.childIndex(0, ONodeId.valueOf(id));
    final ONodeId offset = OMerkleTreeNode.startNodeId(1, childIndex, ONodeId.valueOf(0));

    final OMerkleTreeNode node = roots[childIndex];

    return node.addRecord(1, offset, id, data);
  }

  public void deleteData(final OAutoShardedRecordId id, final ODHTRecordVersion version) {
    final int childIndex = OMerkleTreeNode.childIndex(0, ONodeId.valueOf(id));
    final ONodeId offset = OMerkleTreeNode.startNodeId(1, childIndex, ONodeId.valueOf(0));

    final OMerkleTreeNode node = roots[childIndex];
    node.deleteRecord(1, offset, id, version);
  }

  public void deleteData(final OAutoShardedRecordId id, final ODHTRecordVersion version, boolean softDelete) {
    final int childIndex = OMerkleTreeNode.childIndex(0, ONodeId.valueOf(id));
    final ONodeId startId = OMerkleTreeNode.startNodeId(1, childIndex, ONodeId.valueOf(0));

    final OMerkleTreeNode node = roots[childIndex];
    if (softDelete)
      node.deleteRecord(1, startId, id, version);
    else
      node.cleanOutRecord(1, startId, id, version);
  }

  public void updateData(final OAutoShardedRecordId id, final ODHTRecordVersion version, final String data) {
    final int childIndex = OMerkleTreeNode.childIndex(0, ONodeId.valueOf(id));
    final ONodeId offset = OMerkleTreeNode.startNodeId(1, childIndex, ONodeId.valueOf(0));

    final OMerkleTreeNode node = roots[childIndex];
    node.updateRecord(1, offset, id, version, data);
  }

  public void updateReplica(final OAutoShardedRecordId id, final Record replica) {
    final int childIndex = OMerkleTreeNode.childIndex(0, ONodeId.valueOf(id));
    final ONodeId offset = OMerkleTreeNode.startNodeId(1, childIndex, ONodeId.valueOf(0));

    final OMerkleTreeNode node = roots[childIndex];
    node.updateReplica(1, offset, id, replica);
  }

  public ODetachedMerkleTreeNode getRootNode(int index) {
    ONodeId startId = OMerkleTreeNode.startNodeId(1, index, ONodeId.valueOf(0));
    ONodeId endId = OMerkleTreeNode.startNodeId(1, index + 1, ONodeId.valueOf(0)).subtract(ONodeId.valueOf(1));

    return convertToDetachedNode(new int[] { index }, startId, endId, roots[index]);
  }

  public List<ODetachedMerkleTreeNode> getRootNodesForInterval(final ONodeId startId, final ONodeId endId) {
    final int startChildIndex = OMerkleTreeNode.childIndex(0, startId);
    final int endChildIndex = OMerkleTreeNode.childIndex(0, endId);

    final List<ODetachedMerkleTreeNode> detachedRoots = new ArrayList<ODetachedMerkleTreeNode>();

    for (int childIndex = startChildIndex; childIndex <= endChildIndex; childIndex++) {
      final ONodeId startRootId = OMerkleTreeNode.startNodeId(1, childIndex, ONodeId.valueOf(0));
      final ONodeId endRootId = OMerkleTreeNode.startNodeId(1, childIndex + 1, ONodeId.valueOf(0)).subtract(ONodeId.valueOf(1));

      detachedRoots.add(convertToDetachedNode(new int[] { childIndex }, startRootId, endRootId, roots[childIndex]));
    }

    return detachedRoots;
  }

  public ODetachedMerkleTreeNode getEquivalentNode(ODetachedMerkleTreeNode detachedNode) {
    final OMerkleTreeNode internalNode = getEquivalentInternalNode(detachedNode);
    if (internalNode == null)
      return null;

    return convertToDetachedNode(detachedNode.getPath(), detachedNode.getStartId(), detachedNode.getEndId(), internalNode);
  }

  public ODetachedMerkleTreeNode getChildNode(ODetachedMerkleTreeNode parent, int index) {
    final OMerkleTreeNode parentNode = getEquivalentInternalNode(parent);

    if (parentNode == null)
      return null;

    if (parentNode.isLeaf())
      return null;

    ONodeId startId = OMerkleTreeNode.startNodeId(parent.getLevel() + 1, index, parent.getStartId());
    ONodeId endId = OMerkleTreeNode.startNodeId(parent.getLevel() + +1, index + 1, parent.getStartId())
        .subtract(ONodeId.valueOf(1));

    final int[] childPath = new int[parent.getLevel() + 1];

    System.arraycopy(parent.getPath(), 0, childPath, 0, parent.getLevel());
    childPath[childPath.length - 1] = index;

    return convertToDetachedNode(childPath, startId, endId, parentNode.getChild(index));

  }

  private OMerkleTreeNode getEquivalentInternalNode(ODetachedMerkleTreeNode detachedNode) {
    final int[] parentPath = detachedNode.getPath();

    OMerkleTreeNode node = roots[parentPath[0]];
    ONodeId offset = OMerkleTreeNode.startNodeId(1, parentPath[0], ONodeId.valueOf(0));

    node.acquireReadLock();
    try {
      int currentLevel = 1;

      while (currentLevel != parentPath.length && !node.isLeaf()) {
        final int childIndex = parentPath[currentLevel];
        final OMerkleTreeNode child = node.getChild(childIndex);

        if (child == null)
          return null;

        offset = OMerkleTreeNode.startNodeId(currentLevel + 1, childIndex, offset);

        child.acquireReadLock();

        node.releaseReadLock();
        node = child;

        currentLevel++;
      }

      if (detachedNode.getLevel() == currentLevel)
        return node;
    } finally {
      node.releaseReadLock();
    }

    return null;
  }

  private ODetachedMerkleTreeNode convertToDetachedNode(int path[], ONodeId startId, ONodeId endId, OMerkleTreeNode node) {
    ODetachedMerkleTreeNode result;
    node.acquireReadLock();
    try {
      if (node.isLeaf()) {
        List<RecordMetadata> recordMetadatas = new ArrayList<RecordMetadata>();
        final Iterator<Record> recordIterator = db
            .subMap(ONodeId.convertToRecordId(startId, clusterId), true, ONodeId.convertToRecordId(endId, clusterId), false)
            .values().iterator();

        while (recordIterator.hasNext()) {
          final Record record = recordIterator.next();
          recordMetadatas.add(new RecordMetadata(record.getId(), record.getVersion()));
        }

        RecordMetadata[] metadata = new RecordMetadata[recordMetadatas.size()];
        metadata = recordMetadatas.toArray(metadata);

        result = new ODetachedMerkleTreeNode(node.getHash(), startId, endId, metadata, null, path);
      } else {
        byte[][] childrenHash = new byte[64][];

        for (int i = 0; i < 64; i++) {
          final OMerkleTreeNode treeNode = node.getChild(i);
          childrenHash[i] = treeNode.getHash();
        }

        result = new ODetachedMerkleTreeNode(node.getHash(), startId, endId, null, childrenHash, path);
      }

    } finally {
      node.releaseReadLock();
    }
    return result;
  }
}
