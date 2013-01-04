package com.orientechnologies.orient.server.distributed;

import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.id.OClusterPositionNodeId;
import com.orientechnologies.orient.core.id.ONodeId;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.distributed.merkletree.ODetachedMerkleTreeNode;
import com.orientechnologies.orient.server.distributed.merkletree.OInMemoryMerkleTree;
import com.orientechnologies.orient.server.distributed.merkletree.OMerkleTree;

/**
 * @author Andrey Lomakin
 * @since 02.10.12
 */
@Test
public class MerkleTreeTest {
  public static final int     CLUSTER_ID = 1;
  private ODatabaseDocumentTx db;
  private ODatabaseDocumentTx db2;

  @BeforeMethod
  public void setUp() {
    OGlobalConfiguration.DB_USE_DISTRIBUTED_VERSION.setValue(true);
    OGlobalConfiguration.USE_NODE_ID_CLUSTER_POSITION.setValue(true);
    OGlobalConfiguration.USE_LHPEPS_MEMORY_CLUSTER.setValue(true);
    OGlobalConfiguration.STORAGE_USE_TOMBSTONES.setValue(true);

    db = new ODatabaseDocumentTx("memory:mercleTreeTest");
    db.create();

    db2 = new ODatabaseDocumentTx("memory:mercleTreeTestSecondaryDb");
    db2.create();
  }

  @AfterMethod
  public void tearDown() throws Exception {
    db.drop();
    db2.drop();
  }

  public void testAdd67KeysToNext1024NodeCompareDetachedNodes() throws Exception {
    OMerkleTree tree = new OInMemoryMerkleTree(new ODHTDatabaseLookupImpl("memory:mercleTreeTest", "admin", "admin"), 1);

    for (int i = 0; i < 2; i++) {
      final ODocument doc = new ODocument();
      doc.setIdentity(convertTORID(i));
      doc.field("value", i + "");
      tree.addData(doc.getIdentity(), doc);
    }

    for (int i = 1024; i < 1089; i++) {
      final ODocument doc = new ODocument();
      doc.setIdentity(convertTORID(i));
      doc.field("value", i + "");
      tree.addData(doc.getIdentity(), doc);
    }

    for (long i = 0; i < 2; i++)
      Assert.assertEquals(((ODocument) db.load(convertTORID(i))).field("value"), i + "");

    for (long i = 1024; i < 1089; i++)
      Assert.assertEquals(((ODocument) db.load(convertTORID(i))).field("value"), i + "");

    OMerkleTree sampleTree = new OInMemoryMerkleTree(new ODHTDatabaseLookupImpl("memory:mercleTreeTestSecondaryDb", "admin",
        "admin"), 1);

    for (ODocument doc : db.browseCluster(db.getClusterNameById(CLUSTER_ID), new OClusterPositionNodeId(ONodeId.MIN_VALUE),
        new OClusterPositionNodeId(ONodeId.MAX_VALUE), true))
      sampleTree.updateReplica(doc.getIdentity(), doc);

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

  private ORecordId convertTORID(long i) {
    return new ORecordId(1, new OClusterPositionNodeId(ONodeId.valueOf(i)));
  }
}
