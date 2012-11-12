package com.orientechnologies.orient.server.distributed;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.id.OAutoShardedRecordId;

/**
 * @author Andrey Lomakin
 * @since 10.09.12
 */
@Test
public class MerkleTreeNodeTest {
  public void testAddOneKey() throws Exception {
    NavigableMap<OAutoShardedRecordId, Record> map = new TreeMap<OAutoShardedRecordId, Record>();

    OMerkleTreeNode treeNode = new OMerkleTreeNode(map, 1);
    treeNode.addRecord(1, ONodeId.valueOf(0), ONodeId.convertToRecordId(ONodeId.valueOf(130), 1), "130");

    Record record = map.get(convertToRecordId(130L));
    Assert.assertEquals(record.getData(), "130");
    Assert.assertEquals(record.getShortVersion(), 0);

    MessageDigest sha = MessageDigest.getInstance("SHA-1");

    final ByteBuffer byteBuffer = ByteBuffer.allocate(OMerkleTreeNode.LEAF_BUFFER_ENTRY_SIZE);
    byteBuffer.put(ONodeId.toStream(record.getId()));
    byteBuffer.put(record.getVersion().toStream());

    byteBuffer.rewind();
    sha.update(byteBuffer);

    Assert.assertEquals(treeNode.getHash(), sha.digest());
    Assert.assertEquals(treeNode.getRecordsCount(), 1);
  }

  public void testAdd66Keys() throws Exception {
    NavigableMap<OAutoShardedRecordId, Record> map = new TreeMap<OAutoShardedRecordId, Record>();

    OMerkleTreeNode treeNode = new OMerkleTreeNode(map, 1);
    for (int i = 0; i < 66; i++)
      treeNode.addRecord(1, ONodeId.valueOf(0), convertToRecordId(i), i + "");

    for (long i = 0; i < 66; i++)
      Assert.assertEquals(map.get(convertToRecordId(i)).getData(), i + "");

    OMerkleTreeNode parent = null;

    List<byte[]> hashes = new ArrayList<byte[]>();

    for (int n = 0; n < 30; n++) {
      hashes.add(treeNode.getHash());

      Assert.assertTrue(!treeNode.isLeaf());

      for (int i = 1; i < 64; i++)
        Assert.assertTrue(treeNode.getChild(i).isLeaf());

      parent = treeNode;
      treeNode = treeNode.getChild(0);
    }

    Assert.assertTrue(treeNode.isLeaf());

    treeNode = parent;

    Assert.assertEquals(treeNode.getChild(0).getRecordsCount(), 64);
    Assert.assertEquals(treeNode.getChild(1).getRecordsCount(), 2);

    ByteBuffer buffer = ByteBuffer.allocate(64 * OMerkleTreeNode.LEAF_BUFFER_ENTRY_SIZE);

    OMerkleTreeNode child = treeNode.getChild(0);
    for (int n = 0; n < 64; n++) {
      Record record = map.get(convertToRecordId(n));

      buffer.put(ONodeId.toStream(record.getId()));
      buffer.put(record.getVersion().toStream());
    }

    buffer.limit(buffer.position());
    buffer.rewind();

    MessageDigest sha = MessageDigest.getInstance("SHA-1");
    sha.update(buffer);

    Assert.assertEquals(child.getHash(), sha.digest());

    final ByteBuffer lastChildBuffer = ByteBuffer.allocate(OMerkleTreeNode.LEAF_BUFFER_ENTRY_SIZE * 2);
    Record record;

    record = map.get(convertToRecordId(64L));
    lastChildBuffer.put(ONodeId.toStream(record.getId()));
    lastChildBuffer.put(record.getVersion().toStream());

    record = map.get(convertToRecordId(65L));
    lastChildBuffer.put(ONodeId.toStream(record.getId()));
    lastChildBuffer.put(record.getVersion().toStream());

    lastChildBuffer.rewind();

    MessageDigest lastChildSHA = MessageDigest.getInstance("SHA-1");
    lastChildSHA.update(lastChildBuffer);

    Assert.assertEquals(treeNode.getChild(1).getHash(), lastChildSHA.digest());

    buffer = ByteBuffer.allocate(64 * 20);
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

    for (int i = 28; i >= 0; i--) {
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
    NavigableMap<OAutoShardedRecordId, Record> map = new TreeMap<OAutoShardedRecordId, Record>();

    OMerkleTreeNode treeNode = new OMerkleTreeNode(map, 1);

    for (ONodeId i = ONodeId.MAX_VALUE; i.compareTo(ONodeId.MAX_VALUE.subtract(ONodeId.valueOf(66))) >= 0; i = i.subtract(ONodeId
        .valueOf(1))) {
      int childPos = OMerkleTreeNode.childIndex(0, i);
      ONodeId startKey = OMerkleTreeNode.startNodeId(1, childPos, ONodeId.valueOf(0));

      treeNode.addRecord(1, startKey, ONodeId.convertToRecordId(i, 1), i + "");
    }

    for (ONodeId i = ONodeId.MAX_VALUE; i.compareTo(ONodeId.MAX_VALUE.subtract(ONodeId.valueOf(66))) >= 0; i = i.subtract(ONodeId
        .valueOf(1)))
      Assert.assertEquals(map.get(ONodeId.convertToRecordId(i, 1)).getData(), i + "");

    OMerkleTreeNode parent = null;

    List<byte[]> hashes = new ArrayList<byte[]>();

    for (int n = 0; n < 30; n++) {
      hashes.add(treeNode.getHash());

      Assert.assertTrue(!treeNode.isLeaf());

      for (int i = 0; i < 63; i++)
        Assert.assertTrue(treeNode.getChild(i).isLeaf());

      parent = treeNode;
      treeNode = treeNode.getChild(63);
    }

    Assert.assertTrue(treeNode.isLeaf());

    treeNode = parent;

    Assert.assertEquals(treeNode.getChild(63).getRecordsCount(), 64);

    Assert.assertEquals(treeNode.getChild(62).getRecordsCount(), 3);

    ByteBuffer buffer = ByteBuffer.allocate(64 * OMerkleTreeNode.LEAF_BUFFER_ENTRY_SIZE);

    OMerkleTreeNode child = treeNode.getChild(63);
    for (int n = 63; n >= 0; n--) {
      final ONodeId id = ONodeId.MAX_VALUE.subtract(ONodeId.valueOf(n));

      Record record = map.get(ONodeId.convertToRecordId(id, 1));

      buffer.put(id.toStream());
      buffer.put(record.getVersion().toStream());
    }

    buffer.rewind();

    MessageDigest sha = MessageDigest.getInstance("SHA-1");
    sha.update(buffer);

    Assert.assertEquals(child.getHash(), sha.digest());

    final ByteBuffer lastChildBuffer = ByteBuffer.allocate(OMerkleTreeNode.LEAF_BUFFER_ENTRY_SIZE * 3);

    Record record = map.get(ONodeId.convertToRecordId(ONodeId.MAX_VALUE.subtract(ONodeId.valueOf(66)), 1));

    lastChildBuffer.put(ONodeId.toStream(record.getId()));
    lastChildBuffer.put(record.getVersion().toStream());

    record = map.get(ONodeId.convertToRecordId(ONodeId.MAX_VALUE.subtract(ONodeId.valueOf(65)), 1));
    lastChildBuffer.put(ONodeId.toStream(record.getId()));
    lastChildBuffer.put(record.getVersion().toStream());

    record = map.get(ONodeId.convertToRecordId(ONodeId.MAX_VALUE.subtract(ONodeId.valueOf(64)), 1));
    lastChildBuffer.put(ONodeId.toStream(record.getId()));
    lastChildBuffer.put(record.getVersion().toStream());

    lastChildBuffer.rewind();

    MessageDigest lastChildSHA = MessageDigest.getInstance("SHA-1");
    lastChildSHA.update(lastChildBuffer);

    Assert.assertEquals(treeNode.getChild(62).getHash(), lastChildSHA.digest());

    buffer = ByteBuffer.allocate(64 * 20);
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

    for (int i = 28; i >= 0; i--) {
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
    NavigableMap<OAutoShardedRecordId, Record> map = new TreeMap<OAutoShardedRecordId, Record>();

    OMerkleTreeNode treeNode = new OMerkleTreeNode(map, 1);
    for (int i = 0; i < 2; i++)
      treeNode.addRecord(1, ONodeId.valueOf(0), convertToRecordId(i), i + "");

    for (int i = 1024; i < 1089; i++)
      treeNode.addRecord(1, ONodeId.valueOf(0), convertToRecordId(i), i + "");

    for (long i = 0; i < 2; i++)
      Assert.assertEquals(map.get(convertToRecordId(i)).getData(), i + "");

    for (long i = 1024; i < 1089; i++)
      Assert.assertEquals(map.get(convertToRecordId(i)).getData(), i + "");

    List<byte[]> hashes = new ArrayList<byte[]>();

    for (int n = 0; n < 29; n++) {
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

    treeNode = treeNode.getChild(16);

    Assert.assertTrue(treeNode.isLeaf());

    Assert.assertEquals(treeNode.getRecordsCount(), 64);

    treeNode = parent.getChild(17);
    Assert.assertTrue(treeNode.isLeaf());
    Assert.assertEquals(treeNode.getRecordsCount(), 1);

    ByteBuffer buffer = ByteBuffer.allocate(64 * OMerkleTreeNode.LEAF_BUFFER_ENTRY_SIZE);

    OMerkleTreeNode child = parent.getChild(16);
    for (int n = 0; n < 64; n++) {
      final Record record = map.get(convertToRecordId(1024L + n));

      buffer.put(ONodeId.toStream(record.getId()));
      buffer.put(record.getVersion().toStream());
    }

    buffer.rewind();

    MessageDigest sha = MessageDigest.getInstance("SHA-1");
    sha.update(buffer);

    Assert.assertEquals(child.getHash(), sha.digest());

    Record record = map.get(convertToRecordId(1088L));
    final ByteBuffer lastChildBuffer = ByteBuffer.allocate(OMerkleTreeNode.LEAF_BUFFER_ENTRY_SIZE);

    lastChildBuffer.put(ONodeId.toStream(convertToRecordId(1088)));
    lastChildBuffer.put(record.getVersion().toStream());

    lastChildBuffer.rewind();

    MessageDigest lastChildSHA = MessageDigest.getInstance("SHA-1");
    lastChildSHA.update(lastChildBuffer);

    Assert.assertEquals(parent.getChild(17).getHash(), lastChildSHA.digest());

    MessageDigest firstNodeSHA = MessageDigest.getInstance("SHA-1");

    ByteBuffer firstNodeBuffer = ByteBuffer.allocate(OMerkleTreeNode.LEAF_BUFFER_ENTRY_SIZE * 2);

    record = map.get(convertToRecordId(0));

    firstNodeBuffer.put(ONodeId.toStream(record.getId()));
    firstNodeBuffer.put(record.getVersion().toStream());

    record = map.get(convertToRecordId(1));

    firstNodeBuffer.put(ONodeId.toStream(record.getId()));
    firstNodeBuffer.put(record.getVersion().toStream());

    firstNodeBuffer.rewind();

    firstNodeSHA.update(firstNodeBuffer);

    byte[] firstNodeHash = firstNodeSHA.digest();
    Assert.assertEquals(parent.getChild(0).getHash(), firstNodeHash);

    buffer = ByteBuffer.allocate(64 * 20);
    for (int i = 0; i < 64; i++)
      buffer.put(parent.getChild(i).getHash());

    buffer.rewind();

    MessageDigest hashDigest = MessageDigest.getInstance("SHA-1");
    hashDigest.update(buffer);

    byte[] prevHash = hashDigest.digest();
    Assert.assertEquals(parent.getHash(), prevHash);

    final MessageDigest emptySHA = MessageDigest.getInstance("SHA-1");
    emptySHA.update(new byte[0]);
    final byte[] emptyHash = emptySHA.digest();

    for (int i = 28; i >= 0; i--) {
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
    NavigableMap<OAutoShardedRecordId, Record> mapOne = new TreeMap<OAutoShardedRecordId, Record>();

    OMerkleTreeNode testTreeNode = new OMerkleTreeNode(mapOne, 1);
    for (int i = 0; i < 69; i++)
      testTreeNode.addRecord(1, ONodeId.valueOf(0), convertToRecordId(i), i + "");

    for (long i = 68; i < 69; i++) {
      final Record record = mapOne.get(convertToRecordId(i));
      testTreeNode.deleteRecord(1, ONodeId.valueOf(0), record.getId(), record.getVersion());
    }

    for (long i = 0; i < 66; i++)
      Assert.assertEquals(mapOne.get(convertToRecordId(i)).getData(), i + "");

    NavigableMap<OAutoShardedRecordId, Record> mapTwo = new TreeMap<OAutoShardedRecordId, Record>();
    OMerkleTreeNode sampleTreeNode = new OMerkleTreeNode(mapTwo, 1);

    for (Map.Entry<OAutoShardedRecordId, Record> entry : mapOne.entrySet())
      sampleTreeNode.updateReplica(1, ONodeId.valueOf(0), entry.getKey(), entry.getValue());

    compareNodes(sampleTreeNode, testTreeNode);
  }

  public void testAdd69KeysRemove60() {
    NavigableMap<OAutoShardedRecordId, Record> mapOne = new TreeMap<OAutoShardedRecordId, Record>();

    OMerkleTreeNode testTreeNode = new OMerkleTreeNode(mapOne, 1);
    for (int i = 0; i < 69; i++)
      testTreeNode.addRecord(1, ONodeId.valueOf(0), convertToRecordId(i), i + "");

    for (long i = 64; i < 69; i++) {
      final Record record = mapOne.get(convertToRecordId(i));

      testTreeNode.deleteRecord(1, ONodeId.valueOf(0), convertToRecordId(i), record.getVersion());
    }

    for (long i = 0; i < 64; i++)
      Assert.assertEquals(mapOne.get(convertToRecordId(i)).getData(), i + "");

    NavigableMap<OAutoShardedRecordId, Record> mapTwo = new TreeMap<OAutoShardedRecordId, Record>();
    OMerkleTreeNode sampleTreeNode = new OMerkleTreeNode(mapTwo, 1);

    for (Map.Entry<OAutoShardedRecordId, Record> entry : mapOne.entrySet())
      sampleTreeNode.updateReplica(1, ONodeId.valueOf(0), entry.getKey(), entry.getValue());

    compareNodes(sampleTreeNode, testTreeNode);
  }

  public void testAdd67KeysToNext1024NodeRemove60() throws Exception {
    NavigableMap<OAutoShardedRecordId, Record> map = new TreeMap<OAutoShardedRecordId, Record>();

    OMerkleTreeNode testTreeNode = new OMerkleTreeNode(map, 1);
    for (int i = 0; i < 2; i++)
      testTreeNode.addRecord(1, ONodeId.valueOf(0), convertToRecordId(i), i + "");

    for (int i = 1024; i < 1089; i++)
      testTreeNode.addRecord(1, ONodeId.valueOf(0), convertToRecordId(i), i + "");

    for (long i = 1024; i < 1084; i++) {
      final Record record = map.get(convertToRecordId(i));

      testTreeNode.deleteRecord(1, ONodeId.valueOf(0), convertToRecordId(i), record.getVersion());
    }

    for (long i = 0; i < 2; i++)
      Assert.assertEquals(map.get(convertToRecordId(i)).getData(), i + "");

    for (long i = 1084; i < 1089; i++)
      Assert.assertEquals(map.get(convertToRecordId(i)).getData(), i + "");

    NavigableMap<OAutoShardedRecordId, Record> mapTwo = new TreeMap<OAutoShardedRecordId, Record>();
    OMerkleTreeNode sampleTreeNode = new OMerkleTreeNode(mapTwo, 1);

    for (Map.Entry<OAutoShardedRecordId, Record> entry : map.entrySet())
      sampleTreeNode.updateReplica(1, ONodeId.valueOf(0), entry.getKey(), entry.getValue());

    compareNodes(sampleTreeNode, testTreeNode);
  }

  public void testAdd67KeysToEndRemove60() throws Exception {
    NavigableMap<OAutoShardedRecordId, Record> map = new TreeMap<OAutoShardedRecordId, Record>();

    OMerkleTreeNode testTreeNode = new OMerkleTreeNode(map, 1);

    for (ONodeId i = ONodeId.MAX_VALUE; i.compareTo(ONodeId.MAX_VALUE.subtract(ONodeId.valueOf(66))) >= 0; i = i
        .subtract(ONodeId.ONE)) {
      int childPos = OMerkleTreeNode.childIndex(0, i);
      ONodeId startKey = OMerkleTreeNode.startNodeId(1, childPos, ONodeId.ZERO);

      testTreeNode.addRecord(1, startKey, ONodeId.convertToRecordId(i, 1), i + "");
    }

    for (ONodeId i = ONodeId.MAX_VALUE.subtract(ONodeId.valueOf(66)); i.compareTo(ONodeId.MAX_VALUE.subtract(ONodeId.valueOf(6))) < 0; i = i
        .add(ONodeId.ONE)) {
      final int childPos = OMerkleTreeNode.childIndex(0, i);
      final ONodeId startKey = OMerkleTreeNode.startNodeId(1, childPos, ONodeId.ZERO);

      final Record record = map.get(ONodeId.convertToRecordId(i, 1));

      testTreeNode.deleteRecord(1, startKey, record.getId(), record.getVersion());
    }

    for (ONodeId i = ONodeId.MAX_VALUE.subtract(ONodeId.valueOf(6)); i.compareTo(ONodeId.MIN_VALUE) > 0; i = i.add(ONodeId.ONE))
      Assert.assertEquals(map.get(ONodeId.convertToRecordId(i, 1)).getData(), i + "");

    NavigableMap<OAutoShardedRecordId, Record> mapTwo = new TreeMap<OAutoShardedRecordId, Record>();
    OMerkleTreeNode sampleTreeNode = new OMerkleTreeNode(mapTwo, 1);

    for (Map.Entry<OAutoShardedRecordId, Record> entry : map.entrySet()) {
      int childPos = OMerkleTreeNode.childIndex(0, ONodeId.valueOf(entry.getKey()));
      ONodeId startKey = OMerkleTreeNode.startNodeId(1, childPos, ONodeId.ZERO);

      sampleTreeNode.updateReplica(1, startKey, entry.getKey(), entry.getValue());
    }

    compareNodes(sampleTreeNode, testTreeNode);
  }

  public void testAdd69KeysCleanOut3() {
    NavigableMap<OAutoShardedRecordId, Record> mapOne = new TreeMap<OAutoShardedRecordId, Record>();

    OMerkleTreeNode testTreeNode = new OMerkleTreeNode(mapOne, 1);
    for (int i = 0; i < 69; i++)
      testTreeNode.addRecord(1, ONodeId.ZERO, convertToRecordId(i), i + "");

    for (long i = 68; i < 69; i++) {
      final Record record = mapOne.get(convertToRecordId(i));
      testTreeNode.cleanOutRecord(1, ONodeId.ZERO, record.getId(), record.getVersion());
    }

    for (long i = 0; i < 66; i++)
      Assert.assertEquals(mapOne.get(convertToRecordId(i)).getData(), i + "");

    NavigableMap<OAutoShardedRecordId, Record> mapTwo = new TreeMap<OAutoShardedRecordId, Record>();
    OMerkleTreeNode sampleTreeNode = new OMerkleTreeNode(mapTwo, 1);

    for (Map.Entry<OAutoShardedRecordId, Record> entry : mapOne.entrySet()) {
      if (entry.getValue().isTombstone())
        Assert.fail();

      sampleTreeNode.updateReplica(1, ONodeId.ZERO, entry.getKey(), entry.getValue());
    }

    compareNodes(sampleTreeNode, testTreeNode);
  }

  public void testAdd69KeysCleanOut60() {
    NavigableMap<OAutoShardedRecordId, Record> mapOne = new TreeMap<OAutoShardedRecordId, Record>();

    OMerkleTreeNode testTreeNode = new OMerkleTreeNode(mapOne, 1);
    for (int i = 0; i < 69; i++)
      testTreeNode.addRecord(1, ONodeId.ZERO, convertToRecordId(i), i + "");

    for (long i = 64; i < 69; i++) {
      final Record record = mapOne.get(convertToRecordId(i));

      testTreeNode.cleanOutRecord(1, ONodeId.ZERO, record.getId(), record.getVersion());
    }

    for (long i = 0; i < 64; i++)
      Assert.assertEquals(mapOne.get(convertToRecordId(i)).getData(), i + "");

    NavigableMap<OAutoShardedRecordId, Record> mapTwo = new TreeMap<OAutoShardedRecordId, Record>();
    OMerkleTreeNode sampleTreeNode = new OMerkleTreeNode(mapTwo, 1);

    for (Map.Entry<OAutoShardedRecordId, Record> entry : mapOne.entrySet()) {
      if (entry.getValue().isTombstone())
        Assert.fail();

      sampleTreeNode.updateReplica(1, ONodeId.ZERO, entry.getKey(), entry.getValue());
    }

    compareNodes(sampleTreeNode, testTreeNode);
  }

  public void testAdd67KeysToNext1024NodeCleanOut60() throws Exception {
    NavigableMap<OAutoShardedRecordId, Record> map = new TreeMap<OAutoShardedRecordId, Record>();

    OMerkleTreeNode testTreeNode = new OMerkleTreeNode(map, 1);
    for (int i = 0; i < 2; i++)
      testTreeNode.addRecord(1, ONodeId.ZERO, convertToRecordId(i), i + "");

    for (int i = 1024; i < 1089; i++)
      testTreeNode.addRecord(1, ONodeId.ZERO, convertToRecordId(i), i + "");

    for (long i = 1024; i < 1084; i++) {
      final Record record = map.get(convertToRecordId(i));

      testTreeNode.cleanOutRecord(1, ONodeId.ZERO, record.getId(), record.getVersion());
    }

    for (long i = 0; i < 2; i++)
      Assert.assertEquals(map.get(convertToRecordId(i)).getData(), i + "");

    for (long i = 1084; i < 1089; i++)
      Assert.assertEquals(map.get(convertToRecordId(i)).getData(), i + "");

    NavigableMap<OAutoShardedRecordId, Record> mapTwo = new TreeMap<OAutoShardedRecordId, Record>();
    OMerkleTreeNode sampleTreeNode = new OMerkleTreeNode(mapTwo, 1);

    for (Map.Entry<OAutoShardedRecordId, Record> entry : map.entrySet()) {
      if (entry.getValue().isTombstone())
        Assert.fail();

      sampleTreeNode.updateReplica(1, ONodeId.ZERO, entry.getKey(), entry.getValue());
    }

    compareNodes(sampleTreeNode, testTreeNode);
  }

  public void testAdd67KeysToEndCleanOut60() throws Exception {
    NavigableMap<OAutoShardedRecordId, Record> map = new TreeMap<OAutoShardedRecordId, Record>();

    OMerkleTreeNode testTreeNode = new OMerkleTreeNode(map, 1);

    for (ONodeId i = ONodeId.MAX_VALUE; i.compareTo(ONodeId.MAX_VALUE.subtract(ONodeId.valueOf(66))) >= 0; i = i
        .subtract(ONodeId.ONE)) {
      int childPos = OMerkleTreeNode.childIndex(0, i);
      ONodeId startKey = OMerkleTreeNode.startNodeId(1, childPos, ONodeId.ZERO);

      testTreeNode.addRecord(1, startKey, ONodeId.convertToRecordId(i, 1), i + "");
    }

    for (ONodeId i = ONodeId.MAX_VALUE.subtract(ONodeId.valueOf(66)); i.compareTo(ONodeId.MAX_VALUE.subtract(ONodeId.valueOf(62))) < 0; i = i
        .add(ONodeId.ONE)) {
      final int childPos = OMerkleTreeNode.childIndex(0, i);
      final ONodeId startKey = OMerkleTreeNode.startNodeId(1, childPos, ONodeId.ZERO);

      final Record record = map.get(ONodeId.convertToRecordId(i, 1));

      testTreeNode.cleanOutRecord(1, startKey, record.getId(), record.getVersion());
    }

    for (ONodeId i = ONodeId.MAX_VALUE.subtract(ONodeId.valueOf(60)); i.compareTo(ONodeId.ZERO) > 0; i = i.add(ONodeId.ONE))
      Assert.assertEquals(map.get(ONodeId.convertToRecordId(i, 1)).getData(), i + "");

    NavigableMap<OAutoShardedRecordId, Record> mapTwo = new TreeMap<OAutoShardedRecordId, Record>();
    OMerkleTreeNode sampleTreeNode = new OMerkleTreeNode(mapTwo, 1);

    for (Map.Entry<OAutoShardedRecordId, Record> entry : map.entrySet()) {
      int childPos = OMerkleTreeNode.childIndex(0, ONodeId.valueOf(entry.getKey()));
      ONodeId startKey = OMerkleTreeNode.startNodeId(1, childPos, ONodeId.ZERO);

      if (entry.getValue().isTombstone())
        Assert.fail();

      sampleTreeNode.updateReplica(1, startKey, entry.getKey(), entry.getValue());
    }

    compareNodes(sampleTreeNode, testTreeNode);
  }

  public void testAddOneKeyUpdateOneKey() throws Exception {
    NavigableMap<OAutoShardedRecordId, Record> map = new TreeMap<OAutoShardedRecordId, Record>();

    OMerkleTreeNode treeNode = new OMerkleTreeNode(map, 1);
    treeNode.addRecord(1, ONodeId.ZERO, convertToRecordId(130), "130");

    Record record = map.get(convertToRecordId(130L));

    treeNode.updateRecord(1, ONodeId.ZERO, record.getId(), record.getVersion(), "150");

    record = map.get(convertToRecordId(130L));
    Assert.assertEquals(record.getData(), "150");
    Assert.assertEquals(record.getShortVersion(), 1);

    MessageDigest sha = MessageDigest.getInstance("SHA-1");

    final ByteBuffer byteBuffer = ByteBuffer.allocate(OMerkleTreeNode.LEAF_BUFFER_ENTRY_SIZE);
    byteBuffer.put(ONodeId.valueOf(130L).toStream());
    byteBuffer.put(record.getVersion().toStream());

    byteBuffer.rewind();
    sha.update(byteBuffer);

    Assert.assertEquals(treeNode.getHash(), sha.digest());
    Assert.assertEquals(treeNode.getRecordsCount(), 1);
  }

  public void testAddOneKeyUpdateOneKeyTwoTimes() throws Exception {
    NavigableMap<OAutoShardedRecordId, Record> map = new TreeMap<OAutoShardedRecordId, Record>();

    OMerkleTreeNode treeNode = new OMerkleTreeNode(map, 1);
    treeNode.addRecord(1, ONodeId.ZERO, convertToRecordId(130), "130");

    final Record record = map.get(convertToRecordId(130L));

    treeNode.updateRecord(1, ONodeId.ZERO, convertToRecordId(130), record.getVersion(), "150");
    treeNode.updateRecord(1, ONodeId.ZERO, convertToRecordId(130), record.getVersion(), "160");

    Assert.assertEquals(record.getData(), "160");
    Assert.assertEquals(record.getShortVersion(), 2);

    MessageDigest sha = MessageDigest.getInstance("SHA-1");

    final ByteBuffer byteBuffer = ByteBuffer.allocate(OMerkleTreeNode.LEAF_BUFFER_ENTRY_SIZE);
    byteBuffer.put(ONodeId.valueOf(130L).toStream());
    byteBuffer.put(record.getVersion().toStream());

    byteBuffer.rewind();
    sha.update(byteBuffer);

    Assert.assertEquals(treeNode.getHash(), sha.digest());
    Assert.assertEquals(treeNode.getRecordsCount(), 1);
  }

  public void testAddOneKeyUpdateOneKeySecondTimeWithWrongVersion() throws Exception {
    NavigableMap<OAutoShardedRecordId, Record> map = new TreeMap<OAutoShardedRecordId, Record>();

    OMerkleTreeNode treeNode = new OMerkleTreeNode(map, 1);
    treeNode.addRecord(1, ONodeId.ZERO, convertToRecordId(130), "130");

    Record record = map.get(convertToRecordId(130L));

    treeNode.updateRecord(1, ONodeId.ZERO, record.getId(), record.getVersion(), "150");

    try {
      final ODHTRecordVersion version = new ODHTRecordVersion();
      version.init(34);

      treeNode.updateRecord(1, ONodeId.ZERO, convertToRecordId(130), version, "160");
      Assert.fail();
    } catch (OConcurrentModificationException e) {
    }

    record = map.get(convertToRecordId(130L));
    Assert.assertEquals(record.getData(), "150");
    Assert.assertEquals(record.getShortVersion(), 1);

    MessageDigest sha = MessageDigest.getInstance("SHA-1");

    final ByteBuffer byteBuffer = ByteBuffer.allocate(OMerkleTreeNode.LEAF_BUFFER_ENTRY_SIZE);
    byteBuffer.put(ONodeId.valueOf(130).toStream());
    byteBuffer.put(record.getVersion().toStream());

    byteBuffer.rewind();
    sha.update(byteBuffer);

    Assert.assertEquals(treeNode.getHash(), sha.digest());
    Assert.assertEquals(treeNode.getRecordsCount(), 1);
  }

  public void testAdd69KeysUpdate3() {
    NavigableMap<OAutoShardedRecordId, Record> mapOne = new TreeMap<OAutoShardedRecordId, Record>();

    OMerkleTreeNode testTreeNode = new OMerkleTreeNode(mapOne, 1);
    for (int i = 0; i < 69; i++)
      testTreeNode.addRecord(1, ONodeId.ZERO, convertToRecordId(i), i + "");

    for (long i = 68; i < 69; i++) {
      final Record record = mapOne.get(convertToRecordId(i));

      testTreeNode.updateRecord(1, ONodeId.ZERO, record.getId(), record.getVersion(), "11" + i);
    }

    for (long i = 0; i < 66; i++)
      Assert.assertEquals(mapOne.get(convertToRecordId(i)).getData(), i + "");

    for (long i = 68; i < 69; i++)
      Assert.assertEquals(mapOne.get(convertToRecordId(i)).getData(), "11" + i);

    NavigableMap<OAutoShardedRecordId, Record> mapTwo = new TreeMap<OAutoShardedRecordId, Record>();
    OMerkleTreeNode sampleTreeNode = new OMerkleTreeNode(mapTwo, 1);

    for (Map.Entry<OAutoShardedRecordId, Record> entry : mapOne.entrySet())
      sampleTreeNode.updateReplica(1, ONodeId.ZERO, entry.getKey(), entry.getValue());

    compareNodes(sampleTreeNode, testTreeNode);
  }

  public void testAdd69KeysUpdate60() {
    NavigableMap<OAutoShardedRecordId, Record> mapOne = new TreeMap<OAutoShardedRecordId, Record>();

    OMerkleTreeNode testTreeNode = new OMerkleTreeNode(mapOne, 1);
    for (int i = 0; i < 69; i++)
      testTreeNode.addRecord(1, ONodeId.ZERO, convertToRecordId(i), i + "");

    for (long i = 64; i < 69; i++) {
      final Record record = mapOne.get(convertToRecordId(i));

      testTreeNode.updateRecord(1, ONodeId.ZERO, convertToRecordId(i), record.getVersion(), "11" + i);
    }

    for (long i = 0; i < 64; i++)
      Assert.assertEquals(mapOne.get(convertToRecordId(i)).getData(), i + "");

    for (long i = 64; i < 69; i++)
      Assert.assertEquals(mapOne.get(convertToRecordId(i)).getData(), "11" + i);

    NavigableMap<OAutoShardedRecordId, Record> mapTwo = new TreeMap<OAutoShardedRecordId, Record>();
    OMerkleTreeNode sampleTreeNode = new OMerkleTreeNode(mapTwo, 1);

    for (Map.Entry<OAutoShardedRecordId, Record> entry : mapOne.entrySet())
      sampleTreeNode.updateReplica(1, ONodeId.ZERO, entry.getKey(), entry.getValue());

    compareNodes(sampleTreeNode, testTreeNode);
  }

  public void testAdd67KeysToNext1024NodeUpdate60() throws Exception {
    NavigableMap<OAutoShardedRecordId, Record> map = new TreeMap<OAutoShardedRecordId, Record>();

    OMerkleTreeNode testTreeNode = new OMerkleTreeNode(map, 1);
    for (int i = 0; i < 2; i++)
      testTreeNode.addRecord(1, ONodeId.ZERO, convertToRecordId(i), i + "");

    for (int i = 1024; i < 1089; i++)
      testTreeNode.addRecord(1, ONodeId.ZERO, convertToRecordId(i), i + "");

    for (long i = 1024; i < 1084; i++) {
      final Record record = map.get(convertToRecordId(i));

      testTreeNode.updateRecord(1, ONodeId.ZERO, convertToRecordId(i), record.getVersion(), "11" + i);
    }

    for (long i = 0; i < 2; i++)
      Assert.assertEquals(map.get(convertToRecordId(i)).getData(), i + "");

    for (long i = 1024; i < 1084; i++)
      Assert.assertEquals(map.get(convertToRecordId(i)).getData(), "11" + i);

    for (long i = 1084; i < 1089; i++)
      Assert.assertEquals(map.get(convertToRecordId(i)).getData(), i + "");

    NavigableMap<OAutoShardedRecordId, Record> mapTwo = new TreeMap<OAutoShardedRecordId, Record>();
    OMerkleTreeNode sampleTreeNode = new OMerkleTreeNode(mapTwo, 1);

    for (Map.Entry<OAutoShardedRecordId, Record> entry : map.entrySet()) {
      sampleTreeNode.updateReplica(1, ONodeId.ZERO, entry.getKey(), entry.getValue());
    }

    compareNodes(sampleTreeNode, testTreeNode);
  }

  public void testAdd67KeysToEndUpdate60() throws Exception {
    NavigableMap<OAutoShardedRecordId, Record> map = new TreeMap<OAutoShardedRecordId, Record>();

    OMerkleTreeNode testTreeNode = new OMerkleTreeNode(map, 1);

    for (ONodeId i = ONodeId.MAX_VALUE; i.compareTo(ONodeId.MAX_VALUE.subtract(ONodeId.valueOf(66))) >= 0; i = i
        .subtract(ONodeId.ONE)) {
      int childPos = OMerkleTreeNode.childIndex(0, i);
      ONodeId startKey = OMerkleTreeNode.startNodeId(1, childPos, ONodeId.ZERO);

      testTreeNode.addRecord(1, startKey, ONodeId.convertToRecordId(i, 1), i + "");
    }

    for (ONodeId i = ONodeId.MAX_VALUE.subtract(ONodeId.valueOf(66)); i.compareTo(ONodeId.MAX_VALUE.subtract(ONodeId.valueOf(6))) < 0; i = i
        .add(ONodeId.ONE)) {
      final int childPos = OMerkleTreeNode.childIndex(0, i);
      final ONodeId startKey = OMerkleTreeNode.startNodeId(1, childPos, ONodeId.ZERO);

      final Record record = map.get(ONodeId.convertToRecordId(i, 1));

      testTreeNode.deleteRecord(1, startKey, ONodeId.convertToRecordId(i, 1), record.getVersion());
    }

    for (ONodeId i = ONodeId.MAX_VALUE.subtract(ONodeId.valueOf(6)); i.compareTo(ONodeId.ZERO) > 0; i = i.add(ONodeId.ONE))
      Assert.assertEquals(map.get(ONodeId.convertToRecordId(i, 1)).getData(), i + "");

    NavigableMap<OAutoShardedRecordId, Record> mapTwo = new TreeMap<OAutoShardedRecordId, Record>();
    OMerkleTreeNode sampleTreeNode = new OMerkleTreeNode(mapTwo, 1);

    for (Map.Entry<OAutoShardedRecordId, Record> entry : map.entrySet()) {
      int childPos = OMerkleTreeNode.childIndex(0, ONodeId.valueOf(entry.getKey()));
      ONodeId startKey = OMerkleTreeNode.startNodeId(1, childPos, ONodeId.ZERO);

      sampleTreeNode.updateReplica(1, startKey, entry.getKey(), entry.getValue());
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

  private OAutoShardedRecordId convertToRecordId(long i) {
    return ONodeId.convertToRecordId(ONodeId.valueOf(i), 1);
  }
}
