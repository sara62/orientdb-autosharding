package com.orientechnologies.orient.server.distributed;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

import com.orientechnologies.common.concur.resource.OSharedResourceAdaptive;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.storage.ORecordDuplicatedException;

/**
 * @author Andrey Lomakin
 * @since 04.09.12
 */
public class OMerkleTreeNode extends OSharedResourceAdaptive {
  public static final int                  KEY_SIZE               = 8;
  public static final int                  LEAF_BUFFER_ENTRY_SIZE = KEY_SIZE + ODHTRecordVersion.STREAMED_SIZE;

  private final NavigableMap<Long, Record> db;

  private int                              count;
  private OMerkleTreeNode[]                children;

  private byte[]                           hash;

  public OMerkleTreeNode(final NavigableMap<Long, Record> db) {
    count = 0;
    children = null;

    MessageDigest sha = sha();
    hash = sha.digest();

    this.db = db;
  }

  public OMerkleTreeNode(int count, byte[] hash, NavigableMap<Long, Record> db) {
    this.db = db;
    this.count = count;
    this.hash = hash;
  }

  public Record addRecord(int level, long offset, long id, String data) {
    OMerkleTreeNode treeNode = this;
    final List<PathItem> hashPathNodes = new ArrayList<PathItem>();

    treeNode.acquireExclusiveLock();

    int childIndex = 0;

    while (!treeNode.isLeaf()) {
      hashPathNodes.add(new PathItem(treeNode, childIndex, offset));

      offset = startNodeId(level, childIndex, offset);

      childIndex = childIndex(level, id);

      final OMerkleTreeNode child = treeNode.getChild(childIndex);
      child.acquireExclusiveLock();
      treeNode.releaseExclusiveLock();

      treeNode = child;

      level++;
    }

    Record record;
    try {
      record = db.get(id);

      if (record == null || record.isTombstone()) {
        if (record == null) {
          record = new Record(id, data);
          db.put(id, record);

          treeNode.count++;
        } else {
          record = new Record(id, data, record.getShortVersion() + 1);
          db.put(id, record);
        }

        if (treeNode.getRecordsCount() <= 64)
          rehashLeafNode(level, offset, treeNode, childIndex);
        else {
          final long startId = startNodeId(level, childIndex, offset);

          addInternalNodes(level, startId, treeNode);
          hashPathNodes.add(new PathItem(treeNode, childIndex, offset));
        }
      } else
        throw new ORecordDuplicatedException("Record with id " + id + " has already exist in DB.", new ORecordId(1, id));
    } finally {
      treeNode.releaseExclusiveLock();
    }

    rehashParentNodes(hashPathNodes);

    return record;
  }

  public void updateRecord(int level, long offset, long id, ODHTRecordVersion version, String data) {
    OMerkleTreeNode treeNode = this;
    final List<PathItem> hashPathNodes = new ArrayList<PathItem>();

    treeNode.acquireExclusiveLock();

    int childIndex = 0;

    while (!treeNode.isLeaf()) {
      hashPathNodes.add(new PathItem(treeNode, childIndex, offset));

      offset = startNodeId(level, childIndex, offset);

      childIndex = childIndex(level, id);

      final OMerkleTreeNode child = treeNode.getChild(childIndex);
      child.acquireExclusiveLock();
      treeNode.releaseExclusiveLock();

      treeNode = child;

      level++;
    }

    Record record;
    try {
      record = db.get(id);
      if (record == null || record.isTombstone())
        throw new ORecordNotFoundException("Record with id " + id + " not found.");

      if (record.getVersion().compareTo(version) != 0)
        throw new OConcurrentModificationException(new ORecordId(1, id), record.getShortVersion(), version.getShortVersion(),
            ORecordOperation.UPDATED);

      record.updateData(data, version);

      rehashLeafNode(level, offset, treeNode, childIndex);
    } finally {
      treeNode.releaseExclusiveLock();
    }

    rehashParentNodes(hashPathNodes);
  }

  public boolean updateReplica(int level, long offset, long id, Record replica) {
    OMerkleTreeNode treeNode = this;
    final List<PathItem> hashPathNodes = new ArrayList<PathItem>();

    treeNode.acquireExclusiveLock();

    int childIndex = 0;

    while (!treeNode.isLeaf()) {
      hashPathNodes.add(new PathItem(treeNode, childIndex, offset));

      offset = startNodeId(level, childIndex, offset);

      childIndex = childIndex(level, id);

      final OMerkleTreeNode child = treeNode.getChild(childIndex);
      child.acquireExclusiveLock();
      treeNode.releaseExclusiveLock();

      treeNode = child;

      level++;
    }

    try {
      final Record record = db.get(id);

      if (record == null || replica.compareVersions(record) > 0) {
        db.put(id, replica);

        if (record == null)
          treeNode.count++;

        if (treeNode.getRecordsCount() <= 64)
          rehashLeafNode(level, offset, treeNode, childIndex);
        else {
          final long startId = startNodeId(level, childIndex, offset);

          addInternalNodes(level, startId, treeNode);
          hashPathNodes.add(new PathItem(treeNode, childIndex, offset));
        }
      } else
        return false;
    } finally {
      treeNode.releaseExclusiveLock();
    }

    rehashParentNodes(hashPathNodes);

    return true;
  }

  private void rehashLeafNode(int level, long offset, OMerkleTreeNode treeNode, int childIndex) {
    final MessageDigest messageDigest = sha();
    final ByteBuffer byteBuffer = ByteBuffer.allocate(treeNode.getRecordsCount() * LEAF_BUFFER_ENTRY_SIZE);

    final long startId = startNodeId(level, childIndex, offset);
    final long endId = startNodeId(level, childIndex + 1, offset);

    final Iterator<Long> idIterator;
    if (endId > startId)
      idIterator = db.subMap(startId, true, endId, false).keySet().iterator();
    else
      idIterator = db.tailMap(startId, true).keySet().iterator();

    final int recordsCount = treeNode.getRecordsCount();
    int actualRecordsCount = 0;

    while (idIterator.hasNext()) {
      final long currentId = idIterator.next();
      final ODHTRecordVersion version = db.get(currentId).getVersion();
      byteBuffer.putLong(currentId);
      byteBuffer.put(version.toStream());

      actualRecordsCount++;
    }

    if (actualRecordsCount != recordsCount)
      throw new IllegalStateException("Illegal state of Merkle Tree node. Expected records count is " + recordsCount
          + " but real records count is " + actualRecordsCount);

    byteBuffer.rewind();
    messageDigest.update(byteBuffer);

    treeNode.hash = messageDigest.digest();
  }

  public void deleteRecord(int level, long offset, long id, ODHTRecordVersion version) {
    OMerkleTreeNode treeNode = this;
    final List<PathItem> hashPathNodes = new ArrayList<PathItem>();

    treeNode.acquireExclusiveLock();

    int childIndex = 0;

    while (!treeNode.isLeaf()) {
      hashPathNodes.add(new PathItem(treeNode, childIndex, offset));

      offset = startNodeId(level, childIndex, offset);

      childIndex = childIndex(level, id);

      final OMerkleTreeNode child = treeNode.getChild(childIndex);
      child.acquireExclusiveLock();
      treeNode.releaseExclusiveLock();

      treeNode = child;

      level++;
    }

    try {
      final Record record = db.get(id);
      if (record != null && !record.isTombstone()) {
        if (record.getVersion().compareTo(version) == 0) {
          record.convertToTombstone();

          rehashLeafNode(level, offset, treeNode, childIndex);
        } else
          throw new OConcurrentModificationException(new ORecordId(1, id), record.getShortVersion(), version.getShortVersion(),
              ORecordOperation.UPDATED);
      } else
        throw new ORecordNotFoundException("Record with id " + id + " can not be deleted from database because it is absent");
    } finally {
      treeNode.releaseExclusiveLock();
    }
    rehashParentNodes(hashPathNodes);
  }

  public void cleanOutRecord(int level, long offset, long id, ODHTRecordVersion version) {
    OMerkleTreeNode treeNode = this;
    final List<PathItem> hashPathNodes = new ArrayList<PathItem>();

    treeNode.acquireExclusiveLock();

    int childIndex = 0;

    while (!treeNode.isLeaf()) {
      hashPathNodes.add(new PathItem(treeNode, childIndex, offset));

      offset = startNodeId(level, childIndex, offset);

      childIndex = childIndex(level, id);

      final OMerkleTreeNode child = treeNode.getChild(childIndex);
      child.acquireExclusiveLock();
      treeNode.releaseExclusiveLock();

      treeNode = child;

      level++;
    }

    try {
      final Record record = db.get(id);
      if (record != null && !record.isTombstone()) {
        if (record.getVersion().compareTo(version) == 0) {
          db.remove(id);

          treeNode.count--;

          rehashLeafNode(level, offset, treeNode, childIndex);
        } else
          throw new OConcurrentModificationException(new ORecordId(1, id), record.getShortVersion(), version.getShortVersion(),
              ORecordOperation.UPDATED);
      } else
        throw new ORecordNotFoundException("Record with id " + id + " can not be deleted from database because it is absent");
    } finally {
      treeNode.releaseExclusiveLock();
    }

    rehashParentNodes(hashPathNodes);

  }

  public void acquireReadLock() {
    acquireSharedLock();
  }

  public void releaseReadLock() {
    releaseSharedLock();
  }

  public void acquireWriteLock() {
    acquireExclusiveLock();
  }

  public void releaseWriteLock() {
    releaseExclusiveLock();
  }

  public static long startNodeId(int level, long index, long offset) {
    return (1L << (64 - 6 * level)) * index + offset;
  }

  public static int childIndex(int level, long key) {
    return (int) ((key >> (64 - 6 * (level + 1))) & 63L);
  }

  private void addInternalNodes(int level, long offset, OMerkleTreeNode treeNode) {
    final OMerkleTreeNode[] children = new OMerkleTreeNode[64];
    final ByteBuffer parentBuffer = ByteBuffer.allocate(64 * 20);

    for (int i = 0; i < 64; i++) {
      final int childLevel = level + 1;

      final long startChildKey = startNodeId(childLevel, i, offset);
      final long endChildKey = startNodeId(childLevel, i + 1, offset);

      final Iterator<Long> idIterator;
      if (endChildKey > startChildKey)
        idIterator = db.subMap(startChildKey, true, endChildKey, false).keySet().iterator();
      else
        idIterator = db.tailMap(startChildKey, true).keySet().iterator();

      int recordsCount = 0;

      final Map<Long, ODHTRecordVersion> recordsToHash = new LinkedHashMap<Long, ODHTRecordVersion>(64);
      while (idIterator.hasNext()) {
        if (recordsCount == 64) {
          recordsCount++;
          break;
        }

        final long currentId = idIterator.next();

        final ODHTRecordVersion version = db.get(currentId).getVersion();
        recordsToHash.put(currentId, version);
        recordsCount++;
      }

      final OMerkleTreeNode child;

      if (recordsCount <= 64) {
        final ByteBuffer buffer = ByteBuffer.allocate(recordsCount * LEAF_BUFFER_ENTRY_SIZE);

        for (Map.Entry<Long, ODHTRecordVersion> entry : recordsToHash.entrySet()) {
          buffer.putLong(entry.getKey());
          buffer.put(entry.getValue().toStream());
        }

        buffer.rewind();

        final MessageDigest sha = sha();
        sha.update(buffer);

        child = new OMerkleTreeNode(recordsCount, sha.digest(), db);
      } else {
        child = new OMerkleTreeNode(db);
        addInternalNodes(childLevel, startChildKey, child);
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

  public int getRecordsCount() {
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
            childrenCount += child.getRecordsCount();
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
    private int             childIndex;
    private long            offset;

    private PathItem(OMerkleTreeNode node, int childIndex, long offset) {
      this.node = node;
      this.childIndex = childIndex;
      this.offset = offset;
    }
  }
}
