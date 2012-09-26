package com.orientechnologies.orient.server.distributed;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.*;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.exception.OConcurrentModificationException;

/**
 * @author Andrey Lomakin
 * @since 10.09.12
 */
@Test
public class MerkleTreeNodeTest {
  public void testAddOneKey() throws Exception {
    NavigableMap<Long, Record> map = new TreeMap<Long, Record>();

    OMerkleTreeNode treeNode = new OMerkleTreeNode(map);
    treeNode.addRecord(1, 0, 130, "130");

    Record record = map.get(130L);
    Assert.assertEquals(record.getData(), "130");
    Assert.assertEquals(record.getShortVersion(), 0);

    MessageDigest sha = MessageDigest.getInstance("SHA-1");

    final ByteBuffer byteBuffer = ByteBuffer.allocate(OMerkleTreeNode.LEAF_BUFFER_ENTRY_SIZE);
    byteBuffer.putLong(130L);
    byteBuffer.put(record.getVersion().toStream());

    byteBuffer.rewind();
    sha.update(byteBuffer);

    Assert.assertEquals(treeNode.getHash(), sha.digest());
    Assert.assertEquals(treeNode.getRecordsCount(), 1);
  }

  public void testAdd66Keys() throws Exception {
    NavigableMap<Long, Record> map = new TreeMap<Long, Record>();

    OMerkleTreeNode treeNode = new OMerkleTreeNode(map);
    for (int i = 0; i < 66; i++)
      treeNode.addRecord(1, 0, i, i + "");

    for (long i = 0; i < 66; i++)
      Assert.assertEquals(map.get(i).getData(), i + "");

    OMerkleTreeNode parent = null;

    List<byte[]> hashes = new ArrayList<byte[]>();

    for (int n = 0; n < 9; n++) {
      hashes.add(treeNode.getHash());

      Assert.assertTrue(!treeNode.isLeaf());

      for (int i = 1; i < 64; i++)
        Assert.assertTrue(treeNode.getChild(i).isLeaf());

      parent = treeNode;
      treeNode = treeNode.getChild(0);
    }

    Assert.assertTrue(treeNode.isLeaf());

    treeNode = parent;

    for (int i = 0; i < 4; i++)
      Assert.assertEquals(treeNode.getChild(i).getRecordsCount(), 16);

    Assert.assertEquals(treeNode.getChild(4).getRecordsCount(), 2);

    for (int i = 0; i < 4; i++) {
      final ByteBuffer buffer = ByteBuffer.allocate(16 * OMerkleTreeNode.LEAF_BUFFER_ENTRY_SIZE);

      OMerkleTreeNode child = treeNode.getChild(i);
      for (int n = 0; n < 16; n++) {
        buffer.putLong(i * 16 + n);

        Record record = map.get(i * 16L + n);
        buffer.put(record.getVersion().toStream());
      }

      buffer.limit(buffer.position());
      buffer.rewind();

      MessageDigest sha = MessageDigest.getInstance("SHA-1");
      sha.update(buffer);

      Assert.assertEquals(child.getHash(), sha.digest());
    }

    final ByteBuffer lastChildBuffer = ByteBuffer.allocate(OMerkleTreeNode.LEAF_BUFFER_ENTRY_SIZE * 2);
    Record record;

    lastChildBuffer.putLong(64);
    record = map.get(64L);
    lastChildBuffer.put(record.getVersion().toStream());

    lastChildBuffer.putLong(65);
    record = map.get(65L);
    lastChildBuffer.put(record.getVersion().toStream());

    lastChildBuffer.rewind();

    MessageDigest lastChildSHA = MessageDigest.getInstance("SHA-1");
    lastChildSHA.update(lastChildBuffer);

    Assert.assertEquals(treeNode.getChild(4).getHash(), lastChildSHA.digest());

    ByteBuffer buffer = ByteBuffer.allocate(64 * 20);
    for (int i = 0; i < 64; i++)
      buffer.put(treeNode.getChild(i).getHash());

    buffer.rewind();

    MessageDigest hashDigest = MessageDigest.getInstance("SHA-1");
    hashDigest.update(buffer);

    byte[] prevHash = hashDigest.digest();
    Assert.assertEquals(treeNode.getHash(), prevHash);

    final MessageDigest emptySHA = MessageDigest.getInstance("SHA-1");
    emptySHA.update(new byte[0]);
    final byte[] emptyHash = emptySHA.digest();

    for (int i = 7; i >= 0; i--) {
      buffer = ByteBuffer.allocate(64 * 20);
      buffer.put(prevHash);

      for (int n = 0; n < 63; n++)
        buffer.put(emptyHash);

      buffer.rewind();

      hashDigest = MessageDigest.getInstance("SHA-1");
      hashDigest.update(buffer);

      prevHash = hashDigest.digest();
      Assert.assertEquals(hashes.get(i), prevHash);
    }
  }

  public void testAdd67KeysToEnd() throws Exception {
    NavigableMap<Long, Record> map = new TreeMap<Long, Record>();

    OMerkleTreeNode treeNode = new OMerkleTreeNode(map);

    for (long i = Long.MAX_VALUE; i >= Long.MAX_VALUE - 66; i--) {
      long childPos = OMerkleTreeNode.childIndex(0, i);
      long startKey = OMerkleTreeNode.startNodeId(1, childPos, 0);

      treeNode.addRecord(1, startKey, i, i + "");
    }

    for (long i = Long.MAX_VALUE; i >= Long.MAX_VALUE - 66; i--)
      Assert.assertEquals(map.get(i).getData(), i + "");

    OMerkleTreeNode parent = null;

    List<byte[]> hashes = new ArrayList<byte[]>();

    for (int n = 0; n < 9; n++) {
      hashes.add(treeNode.getHash());

      Assert.assertTrue(!treeNode.isLeaf());

      for (int i = 0; i < 63; i++)
        Assert.assertTrue(treeNode.getChild(i).isLeaf());

      parent = treeNode;
      treeNode = treeNode.getChild(63);
    }

    Assert.assertTrue(treeNode.isLeaf());

    treeNode = parent;

    for (int i = 63; i >= 60; i--)
      Assert.assertEquals(treeNode.getChild(i).getRecordsCount(), 16);

    Assert.assertEquals(treeNode.getChild(59).getRecordsCount(), 3);

    for (int i = 63; i >= 60; i--) {
      final ByteBuffer buffer = ByteBuffer.allocate(16 * OMerkleTreeNode.LEAF_BUFFER_ENTRY_SIZE);

      OMerkleTreeNode child = treeNode.getChild(i);
      for (int n = 15; n >= 0; n--) {
        final long id = Long.MAX_VALUE - n - 16 * (63 - i);

        Record record = map.get(id);

        buffer.putLong(id);
        buffer.put(record.getVersion().toStream());
      }

      buffer.rewind();

      MessageDigest sha = MessageDigest.getInstance("SHA-1");
      sha.update(buffer);

      Assert.assertEquals(child.getHash(), sha.digest());
    }

    final ByteBuffer lastChildBuffer = ByteBuffer.allocate(OMerkleTreeNode.LEAF_BUFFER_ENTRY_SIZE * 3);

    Record record = map.get(Long.MAX_VALUE - 66);

    lastChildBuffer.putLong(Long.MAX_VALUE - 66);
    lastChildBuffer.put(record.getVersion().toStream());

    record = map.get(Long.MAX_VALUE - 65);
    lastChildBuffer.putLong(Long.MAX_VALUE - 65);
    lastChildBuffer.put(record.getVersion().toStream());

    record = map.get(Long.MAX_VALUE - 64);
    lastChildBuffer.putLong(Long.MAX_VALUE - 64);
    lastChildBuffer.put(record.getVersion().toStream());

    lastChildBuffer.rewind();

    MessageDigest lastChildSHA = MessageDigest.getInstance("SHA-1");
    lastChildSHA.update(lastChildBuffer);

    Assert.assertEquals(treeNode.getChild(59).getHash(), lastChildSHA.digest());

    ByteBuffer buffer = ByteBuffer.allocate(64 * 20);
    for (int i = 0; i < 64; i++)
      buffer.put(treeNode.getChild(i).getHash());

    buffer.rewind();

    MessageDigest hashDigest = MessageDigest.getInstance("SHA-1");
    hashDigest.update(buffer);

    byte[] prevHash = hashDigest.digest();
    Assert.assertEquals(treeNode.getHash(), prevHash);

    final MessageDigest emptySHA = MessageDigest.getInstance("SHA-1");
    emptySHA.update(new byte[0]);
    final byte[] emptyHash = emptySHA.digest();

    for (int i = 7; i >= 0; i--) {
      buffer = ByteBuffer.allocate(64 * 20);

      for (int n = 0; n < 63; n++)
        buffer.put(emptyHash);

      buffer.put(prevHash);

      buffer.rewind();

      hashDigest = MessageDigest.getInstance("SHA-1");
      hashDigest.update(buffer);

      prevHash = hashDigest.digest();

      Assert.assertEquals(hashes.get(i), prevHash);
    }
  }

  public void testAdd67KeysToNext1024Node() throws Exception {
    NavigableMap<Long, Record> map = new TreeMap<Long, Record>();

    OMerkleTreeNode treeNode = new OMerkleTreeNode(map);
    for (int i = 0; i < 2; i++)
      treeNode.addRecord(1, 0, i, i + "");

    for (int i = 1024; i < 1089; i++)
      treeNode.addRecord(1, 0, i, i + "");

    for (long i = 0; i < 2; i++)
      Assert.assertEquals(map.get(i).getData(), i + "");

    for (long i = 1024; i < 1089; i++)
      Assert.assertEquals(map.get(i).getData(), i + "");

    List<byte[]> hashes = new ArrayList<byte[]>();

    for (int n = 0; n < 7; n++) {
      hashes.add(treeNode.getHash());

      Assert.assertTrue(!treeNode.isLeaf());

      for (int i = 1; i < 64; i++)
        Assert.assertTrue(treeNode.getChild(i).isLeaf());

      treeNode = treeNode.getChild(0);
    }

    Assert.assertTrue(!treeNode.isLeaf());

    OMerkleTreeNode parent = treeNode;
    Assert.assertTrue(parent.getChild(0).isLeaf());
    Assert.assertEquals(parent.getChild(0).getRecordsCount(), 2);

    treeNode = treeNode.getChild(1);

    Assert.assertTrue(!treeNode.isLeaf());

    for (int i = 0; i < 4; i++)
      Assert.assertEquals(treeNode.getChild(i).getRecordsCount(), 16);

    Assert.assertEquals(treeNode.getChild(4).getRecordsCount(), 1);

    for (int i = 0; i < 4; i++) {
      final ByteBuffer buffer = ByteBuffer.allocate(16 * OMerkleTreeNode.LEAF_BUFFER_ENTRY_SIZE);

      OMerkleTreeNode child = treeNode.getChild(i);
      for (int n = 0; n < 16; n++) {
        buffer.putLong(1024 + i * 16 + n);

        final Record record = map.get(1024L + i * 16 + n);
        buffer.put(record.getVersion().toStream());
      }

      buffer.rewind();

      MessageDigest sha = MessageDigest.getInstance("SHA-1");
      sha.update(buffer);

      Assert.assertEquals(child.getHash(), sha.digest());
    }

    Record record = map.get(1088L);
    final ByteBuffer lastChildBuffer = ByteBuffer.allocate(OMerkleTreeNode.LEAF_BUFFER_ENTRY_SIZE);

    lastChildBuffer.putLong(1088);
    lastChildBuffer.put(record.getVersion().toStream());

    lastChildBuffer.rewind();

    MessageDigest lastChildSHA = MessageDigest.getInstance("SHA-1");
    lastChildSHA.update(lastChildBuffer);

    Assert.assertEquals(treeNode.getChild(4).getHash(), lastChildSHA.digest());

    ByteBuffer buffer = ByteBuffer.allocate(64 * 20);
    for (int i = 0; i < 64; i++)
      buffer.put(treeNode.getChild(i).getHash());

    buffer.rewind();

    MessageDigest hashDigest = MessageDigest.getInstance("SHA-1");
    hashDigest.update(buffer);

    byte[] prevHash = hashDigest.digest();
    Assert.assertEquals(treeNode.getHash(), prevHash);

    MessageDigest firstNodeSHA = MessageDigest.getInstance("SHA-1");

    ByteBuffer firstNodeBuffer = ByteBuffer.allocate(OMerkleTreeNode.LEAF_BUFFER_ENTRY_SIZE * 2);

    record = map.get(0L);

    firstNodeBuffer.putLong(0);
    firstNodeBuffer.put(record.getVersion().toStream());

    record = map.get(1L);

    firstNodeBuffer.putLong(1);
    firstNodeBuffer.put(record.getVersion().toStream());

    firstNodeBuffer.rewind();

    firstNodeSHA.update(firstNodeBuffer);

    byte[] firstNodeHash = firstNodeSHA.digest();
    Assert.assertEquals(parent.getChild(0).getHash(), firstNodeHash);

    final MessageDigest emptySHA = MessageDigest.getInstance("SHA-1");
    emptySHA.update(new byte[0]);
    final byte[] emptyHash = emptySHA.digest();

    final ByteBuffer parentBuffer = ByteBuffer.allocate(64 * 20);

    parentBuffer.put(firstNodeHash);
    parentBuffer.put(prevHash);

    for (int i = 0; i < 62; i++)
      parentBuffer.put(emptyHash);

    parentBuffer.rewind();

    MessageDigest parentSHA = MessageDigest.getInstance("SHA-1");
    parentSHA.update(parentBuffer);

    prevHash = parentSHA.digest();

    Assert.assertEquals(parent.getHash(), prevHash);

    for (int i = 6; i >= 0; i--) {
      buffer = ByteBuffer.allocate(64 * 20);
      buffer.put(prevHash);

      for (int n = 0; n < 63; n++)
        buffer.put(emptyHash);

      buffer.rewind();

      hashDigest = MessageDigest.getInstance("SHA-1");
      hashDigest.update(buffer);

      prevHash = hashDigest.digest();
      Assert.assertEquals(hashes.get(i), prevHash);
    }
  }

  public void testAdd69KeysRemove3() {
    NavigableMap<Long, Record> mapOne = new TreeMap<Long, Record>();

    OMerkleTreeNode testTreeNode = new OMerkleTreeNode(mapOne);
    for (int i = 0; i < 69; i++)
      testTreeNode.addRecord(1, 0, i, i + "");

    for (int i = 68; i < 69; i++)
      testTreeNode.deleteRecord(1, 0, i, 0);

    for (long i = 0; i < 66; i++)
      Assert.assertEquals(mapOne.get(i).getData(), i + "");

    NavigableMap<Long, Record> mapTwo = new TreeMap<Long, Record>();
    OMerkleTreeNode sampleTreeNode = new OMerkleTreeNode(mapTwo);

    for (Map.Entry<Long, Record> entry : mapOne.entrySet())
      sampleTreeNode.putReplica(1, 0, entry.getKey(), entry.getValue());

    compareNodes(sampleTreeNode, testTreeNode);
  }

  public void testAdd69KeysRemove60() {
    NavigableMap<Long, Record> mapOne = new TreeMap<Long, Record>();

    OMerkleTreeNode testTreeNode = new OMerkleTreeNode(mapOne);
    for (int i = 0; i < 69; i++)
      testTreeNode.addRecord(1, 0, i, i + "");

    for (int i = 64; i < 69; i++)
      testTreeNode.deleteRecord(1, 0, i, 0);

    for (long i = 0; i < 64; i++)
      Assert.assertEquals(mapOne.get(i).getData(), i + "");

    NavigableMap<Long, Record> mapTwo = new TreeMap<Long, Record>();
    OMerkleTreeNode sampleTreeNode = new OMerkleTreeNode(mapTwo);

    for (Map.Entry<Long, Record> entry : mapOne.entrySet())
      sampleTreeNode.putReplica(1, 0, entry.getKey(), entry.getValue());

    compareNodes(sampleTreeNode, testTreeNode);
  }

  public void testAdd67KeysToNext1024NodeRemove60() throws Exception {
    NavigableMap<Long, Record> map = new TreeMap<Long, Record>();

    OMerkleTreeNode testTreeNode = new OMerkleTreeNode(map);
    for (int i = 0; i < 2; i++)
      testTreeNode.addRecord(1, 0, i, i + "");

    for (int i = 1024; i < 1089; i++)
      testTreeNode.addRecord(1, 0, i, i + "");

    for (int i = 1024; i < 1084; i++)
      testTreeNode.deleteRecord(1, 0, i, 0);

    for (long i = 0; i < 2; i++)
      Assert.assertEquals(map.get(i).getData(), i + "");

    for (long i = 1084; i < 1089; i++)
      Assert.assertEquals(map.get(i).getData(), i + "");

    NavigableMap<Long, Record> mapTwo = new TreeMap<Long, Record>();
    OMerkleTreeNode sampleTreeNode = new OMerkleTreeNode(mapTwo);

    for (Map.Entry<Long, Record> entry : map.entrySet()) {
      sampleTreeNode.putReplica(1, 0, entry.getKey(), entry.getValue());
    }

    compareNodes(sampleTreeNode, testTreeNode);
  }

  public void testAdd67KeysToEndRemove60() throws Exception {
    NavigableMap<Long, Record> map = new TreeMap<Long, Record>();

    OMerkleTreeNode testTreeNode = new OMerkleTreeNode(map);

    for (long i = Long.MAX_VALUE; i >= Long.MAX_VALUE - 66; i--) {
      long childPos = OMerkleTreeNode.childIndex(0, i);
      long startKey = OMerkleTreeNode.startNodeId(1, childPos, 0);

      testTreeNode.addRecord(1, startKey, i, i + "");
    }

    for (long i = Long.MAX_VALUE - 66; i < Long.MAX_VALUE - 62; i++) {
      final long childPos = OMerkleTreeNode.childIndex(0, i);
      final long startKey = OMerkleTreeNode.startNodeId(1, childPos, 0);

      testTreeNode.deleteRecord(1, startKey, i, 0);
    }

    for (long i = Long.MAX_VALUE - 60; i > 0; i++)
      Assert.assertEquals(map.get(i).getData(), i + "");

    NavigableMap<Long, Record> mapTwo = new TreeMap<Long, Record>();
    OMerkleTreeNode sampleTreeNode = new OMerkleTreeNode(mapTwo);

    for (Map.Entry<Long, Record> entry : map.entrySet()) {
      long childPos = OMerkleTreeNode.childIndex(0, entry.getKey());
      long startKey = OMerkleTreeNode.startNodeId(1, childPos, 0);

      sampleTreeNode.putReplica(1, startKey, entry.getKey(), entry.getValue());
    }

    compareNodes(sampleTreeNode, testTreeNode);
  }

  public void testAddOneKeyUpdateOneKey() throws Exception {
    NavigableMap<Long, Record> map = new TreeMap<Long, Record>();

    OMerkleTreeNode treeNode = new OMerkleTreeNode(map);
    treeNode.addRecord(1, 0, 130, "130");

    treeNode.updateRecord(1, 0, 130, 0, "150");

    Record record = map.get(130L);
    Assert.assertEquals(record.getData(), "150");
    Assert.assertEquals(record.getShortVersion(), 1);

    MessageDigest sha = MessageDigest.getInstance("SHA-1");

    final ByteBuffer byteBuffer = ByteBuffer.allocate(OMerkleTreeNode.LEAF_BUFFER_ENTRY_SIZE);
    byteBuffer.putLong(130L);
    byteBuffer.put(record.getVersion().toStream());

    byteBuffer.rewind();
    sha.update(byteBuffer);

    Assert.assertEquals(treeNode.getHash(), sha.digest());
    Assert.assertEquals(treeNode.getRecordsCount(), 1);
  }

  public void testAddOneKeyUpdateOneKeyTwoTimes() throws Exception {
    NavigableMap<Long, Record> map = new TreeMap<Long, Record>();

    OMerkleTreeNode treeNode = new OMerkleTreeNode(map);
    treeNode.addRecord(1, 0, 130, "130");

    treeNode.updateRecord(1, 0, 130, 0, "150");
    treeNode.updateRecord(1, 0, 130, 1, "160");

    Record record = map.get(130L);
    Assert.assertEquals(record.getData(), "160");
    Assert.assertEquals(record.getShortVersion(), 2);

    MessageDigest sha = MessageDigest.getInstance("SHA-1");

    final ByteBuffer byteBuffer = ByteBuffer.allocate(OMerkleTreeNode.LEAF_BUFFER_ENTRY_SIZE);
    byteBuffer.putLong(130L);
    byteBuffer.put(record.getVersion().toStream());

    byteBuffer.rewind();
    sha.update(byteBuffer);

    Assert.assertEquals(treeNode.getHash(), sha.digest());
    Assert.assertEquals(treeNode.getRecordsCount(), 1);
  }

  public void testAddOneKeyUpdateOneKeySecondTimeWithWrongVersion() throws Exception {
    NavigableMap<Long, Record> map = new TreeMap<Long, Record>();

    OMerkleTreeNode treeNode = new OMerkleTreeNode(map);
    treeNode.addRecord(1, 0, 130, "130");

    treeNode.updateRecord(1, 0, 130, 0, "150");

    try {
      treeNode.updateRecord(1, 0, 130, 3, "160");
      Assert.fail();
    } catch (OConcurrentModificationException e) {
    }

    Record record = map.get(130L);
    Assert.assertEquals(record.getData(), "150");
    Assert.assertEquals(record.getShortVersion(), 1);

    MessageDigest sha = MessageDigest.getInstance("SHA-1");

    final ByteBuffer byteBuffer = ByteBuffer.allocate(OMerkleTreeNode.LEAF_BUFFER_ENTRY_SIZE);
    byteBuffer.putLong(130L);
    byteBuffer.put(record.getVersion().toStream());

    byteBuffer.rewind();
    sha.update(byteBuffer);

    Assert.assertEquals(treeNode.getHash(), sha.digest());
    Assert.assertEquals(treeNode.getRecordsCount(), 1);
  }

  public void testAdd69KeysUpdate3() {
    NavigableMap<Long, Record> mapOne = new TreeMap<Long, Record>();

    OMerkleTreeNode testTreeNode = new OMerkleTreeNode(mapOne);
    for (int i = 0; i < 69; i++)
      testTreeNode.addRecord(1, 0, i, i + "");

    for (int i = 68; i < 69; i++)
      testTreeNode.updateRecord(1, 0, i, 0, "11" + i);

    for (long i = 0; i < 66; i++)
      Assert.assertEquals(mapOne.get(i).getData(), i + "");

    for (long i = 68; i < 69; i++)
      Assert.assertEquals(mapOne.get(i).getData(), "11" + i);

    NavigableMap<Long, Record> mapTwo = new TreeMap<Long, Record>();
    OMerkleTreeNode sampleTreeNode = new OMerkleTreeNode(mapTwo);

    for (Map.Entry<Long, Record> entry : mapOne.entrySet())
      sampleTreeNode.putReplica(1, 0, entry.getKey(), entry.getValue());

    compareNodes(sampleTreeNode, testTreeNode);
  }

  public void testAdd69KeysUpdate60() {
    NavigableMap<Long, Record> mapOne = new TreeMap<Long, Record>();

    OMerkleTreeNode testTreeNode = new OMerkleTreeNode(mapOne);
    for (int i = 0; i < 69; i++)
      testTreeNode.addRecord(1, 0, i, i + "");

    for (int i = 64; i < 69; i++)
      testTreeNode.updateRecord(1, 0, i, 0, "11" + i);

    for (long i = 0; i < 64; i++)
      Assert.assertEquals(mapOne.get(i).getData(), i + "");

    for (long i = 64; i < 69; i++)
      Assert.assertEquals(mapOne.get(i).getData(), "11" + i);

    NavigableMap<Long, Record> mapTwo = new TreeMap<Long, Record>();
    OMerkleTreeNode sampleTreeNode = new OMerkleTreeNode(mapTwo);

    for (Map.Entry<Long, Record> entry : mapOne.entrySet())
      sampleTreeNode.putReplica(1, 0, entry.getKey(), entry.getValue());

    compareNodes(sampleTreeNode, testTreeNode);
  }

  public void testAdd67KeysToNext1024NodeUpdate60() throws Exception {
    NavigableMap<Long, Record> map = new TreeMap<Long, Record>();

    OMerkleTreeNode testTreeNode = new OMerkleTreeNode(map);
    for (int i = 0; i < 2; i++)
      testTreeNode.addRecord(1, 0, i, i + "");

    for (int i = 1024; i < 1089; i++)
      testTreeNode.addRecord(1, 0, i, i + "");

    for (int i = 1024; i < 1084; i++)
      testTreeNode.updateRecord(1, 0, i, 0, "11" + i);

    for (long i = 0; i < 2; i++)
      Assert.assertEquals(map.get(i).getData(), i + "");

    for (long i = 1024; i < 1084; i++)
      Assert.assertEquals(map.get(i).getData(), "11" + i);

    for (long i = 1084; i < 1089; i++)
      Assert.assertEquals(map.get(i).getData(), i + "");

    NavigableMap<Long, Record> mapTwo = new TreeMap<Long, Record>();
    OMerkleTreeNode sampleTreeNode = new OMerkleTreeNode(mapTwo);

    for (Map.Entry<Long, Record> entry : map.entrySet()) {
      sampleTreeNode.putReplica(1, 0, entry.getKey(), entry.getValue());
    }

    compareNodes(sampleTreeNode, testTreeNode);
  }

  public void testAdd67KeysToEndUpdate60() throws Exception {
    NavigableMap<Long, Record> map = new TreeMap<Long, Record>();

    OMerkleTreeNode testTreeNode = new OMerkleTreeNode(map);

    for (long i = Long.MAX_VALUE; i >= Long.MAX_VALUE - 66; i--) {
      long childPos = OMerkleTreeNode.childIndex(0, i);
      long startKey = OMerkleTreeNode.startNodeId(1, childPos, 0);

      testTreeNode.addRecord(1, startKey, i, i + "");
    }

    for (long i = Long.MAX_VALUE - 66; i < Long.MAX_VALUE - 62; i++) {
      final long childPos = OMerkleTreeNode.childIndex(0, i);
      final long startKey = OMerkleTreeNode.startNodeId(1, childPos, 0);

      testTreeNode.deleteRecord(1, startKey, i, 0);
    }

    for (long i = Long.MAX_VALUE - 60; i > 0; i++)
      Assert.assertEquals(map.get(i).getData(), i + "");

    NavigableMap<Long, Record> mapTwo = new TreeMap<Long, Record>();
    OMerkleTreeNode sampleTreeNode = new OMerkleTreeNode(mapTwo);

    for (Map.Entry<Long, Record> entry : map.entrySet()) {
      long childPos = OMerkleTreeNode.childIndex(0, entry.getKey());
      long startKey = OMerkleTreeNode.startNodeId(1, childPos, 0);

      sampleTreeNode.putReplica(1, startKey, entry.getKey(), entry.getValue());
    }

    compareNodes(sampleTreeNode, testTreeNode);
  }

  private void compareNodes(OMerkleTreeNode nodeOne, OMerkleTreeNode nodeTwo) {
    Assert.assertEquals(nodeOne.getHash(), nodeTwo.getHash());
    Assert.assertEquals(nodeOne.isLeaf(), nodeTwo.isLeaf());
    Assert.assertEquals(nodeOne.getRecordsCount(), nodeTwo.getRecordsCount());

    if (!nodeOne.isLeaf()) {
      for (int i = 0; i < 64; i++) {
        final OMerkleTreeNode childOne = nodeOne.getChild(i);
        final OMerkleTreeNode childTwo = nodeTwo.getChild(i);

        compareNodes(childOne, childTwo);
      }
    }
  }
}
