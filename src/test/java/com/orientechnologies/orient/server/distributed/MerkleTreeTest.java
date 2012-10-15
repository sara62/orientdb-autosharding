package com.orientechnologies.orient.server.distributed;

import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * @author Andrey Lomakin
 * @since 02.10.12
 */
@Test
public class MerkleTreeTest {
  public void testAdd67KeysToNext1024NodeCompareDetachedNodes() throws Exception {
    NavigableMap<Long, Record> map = new TreeMap<Long, Record>();

    OMerkleTree tree = new OMerkleTree(map);
    for (int i = 0; i < 2; i++)
      tree.addData(i, i + "");

    for (int i = 1024; i < 1089; i++)
      tree.addData(i, i + "");

    for (long i = 0; i < 2; i++)
      Assert.assertEquals(map.get(i).getData(), i + "");

    for (long i = 1024; i < 1089; i++)
      Assert.assertEquals(map.get(i).getData(), i + "");

    NavigableMap<Long, Record> mapTwo = new TreeMap<Long, Record>();
    OMerkleTree sampleTree = new OMerkleTree(mapTwo);

    for (Map.Entry<Long, Record> entry : map.entrySet())
      sampleTree.updateReplica(entry.getKey(), entry.getValue());

    for (int i = 0; i < 64; i++)
      compareNodes(tree, tree.getRootNode(i), sampleTree, sampleTree.getRootNode(i));
  }

  private void compareNodes(OMerkleTree treeOne, ODetachedMerkleTreeNode nodeOne, OMerkleTree treeTwo,
      ODetachedMerkleTreeNode nodeTwo) {
    Assert.assertEquals(nodeOne.getLevel(), nodeTwo.getLevel());
    Assert.assertEquals(nodeOne.getChildIndex(), nodeTwo.getChildIndex());
    Assert.assertEquals(nodeOne.getHash(), nodeTwo.getHash());
    Assert.assertEquals(nodeOne.getStartId(), nodeTwo.getStartId());
    Assert.assertEquals(nodeOne.getEndId(), nodeTwo.getEndId());

    Assert.assertEquals(nodeOne.isLeaf(), nodeTwo.isLeaf());

    if (nodeOne.isLeaf()) {
      Assert.assertEquals(nodeOne.getRecordsCount(), nodeTwo.getRecordsCount());

      for (int i = 0; i < nodeOne.getRecordsCount(); i++) {
        RecordMetadata metadataOne = nodeOne.getRecordMetadata(i);
        RecordMetadata metadataTwo = nodeTwo.getRecordMetadata(i);

        Assert.assertEquals(metadataOne.getId(), metadataTwo.getId());
        Assert.assertEquals(metadataOne.getVersion(), metadataTwo.getVersion());
      }
    } else {
      for (int i = 0; i < 64; i++) {
        final byte[] childHashOne = nodeOne.getChildHash(i);
        final byte[] childHashTwo = nodeTwo.getChildHash(i);

        Assert.assertEquals(childHashOne, childHashTwo);
      }

      for (int i = 0; i < 64; i++) {
        ODetachedMerkleTreeNode detachedMerkleTreeNodeOne = treeOne.getChildNode(nodeOne, i);
        ODetachedMerkleTreeNode detachedMerkleTreeNodeTwo = treeTwo.getChildNode(nodeTwo, i);

        compareNodes(treeOne, detachedMerkleTreeNodeOne, treeTwo, detachedMerkleTreeNodeTwo);
      }
    }
  }
}
