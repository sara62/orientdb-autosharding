package com.orientechnologies.orient.server.distributed;

import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.server.distributed.merkletree.ODetachedMerkleTreeNode;
import com.orientechnologies.orient.server.distributed.merkletree.OInMemoryMerkleTree;
import com.orientechnologies.orient.server.distributed.merkletree.OMerkleTree;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.id.OClusterPositionNodeId;
import com.orientechnologies.orient.core.id.ONodeId;
import com.orientechnologies.orient.core.id.ORID;

/**
 * @author Andrey Lomakin
 * @since 02.10.12
 */
@Test
public class MerkleTreeTest {
  public void testAdd67KeysToNext1024NodeCompareDetachedNodes() throws Exception {
    NavigableMap<ORID, Record> map = new TreeMap<ORID, Record>();

    OMerkleTree tree = new OInMemoryMerkleTree(map, 1);
    for (int i = 0; i < 2; i++)
      tree.addData(convertTORID(i), i + "");

    for (int i = 1024; i < 1089; i++)
      tree.addData(convertTORID(i), i + "");

    for (long i = 0; i < 2; i++)
      Assert.assertEquals(map.get(convertTORID(i)).getData(), i + "");

    for (long i = 1024; i < 1089; i++)
      Assert.assertEquals(map.get(convertTORID(i)).getData(), i + "");

    NavigableMap<ORID, Record> mapTwo = new TreeMap<ORID, Record>();
    OMerkleTree sampleTree = new OInMemoryMerkleTree(mapTwo, 1);

    for (Map.Entry<ORID, Record> entry : map.entrySet())
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
        ORecordMetadata metadataOne = nodeOne.getRecordMetadata(i);
        ORecordMetadata metadataTwo = nodeTwo.getRecordMetadata(i);

        Assert.assertEquals(metadataOne.getRid(), metadataTwo.getRid());
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

  private ORID convertTORID(long i) {
    return new ORecordId(1, new OClusterPositionNodeId(ONodeId.valueOf(i)));
  }
}
