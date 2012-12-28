package com.orientechnologies.orient.server.distributed;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.version.ODistributedVersion;
import com.orientechnologies.orient.core.version.ORecordVersion;
import com.orientechnologies.orient.server.distributed.merkletree.OMerkleTreeNode;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.id.OClusterPositionNodeId;
import com.orientechnologies.orient.core.id.ONodeId;
import com.orientechnologies.orient.core.id.ORID;

/**
 * @author Andrey Lomakin
 * @since 10.09.12
 */
@Test
public class MerkleTreeNodeTest {

  public static final int CLUSTER_ID = 1;
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

  public void testAddOneKey() throws Exception {
    OMerkleTreeNode treeNode = new OMerkleTreeNode(createDbLookup(), 1);
    final ODocument data = new ODocument().field("value", "130");
    data.setIdentity(new ORecordId(1, new OClusterPositionNodeId(ONodeId.valueOf(130))));
    treeNode.addRecord(1, ONodeId.valueOf(0), data);

    ODocument record = db.load(convertTORID(130L));
    Assert.assertEquals(record.field("value"), "130");
    Assert.assertEquals(record.getRecordVersion().getCounter(), 0);

    MessageDigest sha = MessageDigest.getInstance("SHA-1");

    final ByteBuffer byteBuffer = ByteBuffer.allocate(OMerkleTreeNode.LEAF_BUFFER_ENTRY_SIZE);
    byteBuffer.put(((OClusterPositionNodeId) record.getIdentity().getClusterPosition()).getNodeId().chunksToByteArray());
    byteBuffer.put(record.getRecordVersion().getSerializer().toByteArray(record.getRecordVersion()));

    byteBuffer.rewind();
    sha.update(byteBuffer);

    Assert.assertEquals(treeNode.getHash(), sha.digest());
    Assert.assertEquals(treeNode.getRecordsCount(), 1);
  }

  public void testAdd66Keys() throws Exception {
		OMerkleTreeNode treeNode = new OMerkleTreeNode(createDbLookup(), 1);
		for (int i = 0; i < 66; i++) {
      final ODocument doc = new ODocument();
      doc.field("value", i + "");
      doc.setIdentity(convertTORID(i));
      treeNode.addRecord(1, ONodeId.valueOf(0), doc);
    }

		for (long i = 0; i < 66; i++)
			Assert.assertEquals(((ODocument) db.load(convertTORID(i))).field("value"), i + "");

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
			ODocument record = db.load(convertTORID(n));

			buffer.put(((OClusterPositionNodeId) record.getIdentity().getClusterPosition()).getNodeId().chunksToByteArray());
			buffer.put(record.getRecordVersion().getSerializer().toByteArray(record.getRecordVersion()));
		}

		buffer.limit(buffer.position());
		buffer.rewind();

		MessageDigest sha = MessageDigest.getInstance("SHA-1");
		sha.update(buffer);

		Assert.assertEquals(child.getHash(), sha.digest());

		final ByteBuffer lastChildBuffer = ByteBuffer.allocate(OMerkleTreeNode.LEAF_BUFFER_ENTRY_SIZE * 2);
		ODocument record;

		record = db.load(convertTORID(64L));
		lastChildBuffer.put(((OClusterPositionNodeId) record.getIdentity().getClusterPosition()).getNodeId().chunksToByteArray());
		lastChildBuffer.put(record.getRecordVersion().getSerializer().toByteArray(record.getRecordVersion()));

		record = db.load(convertTORID(65L));
		lastChildBuffer.put(((OClusterPositionNodeId) record.getIdentity().getClusterPosition()).getNodeId().chunksToByteArray());
		lastChildBuffer.put(record.getRecordVersion().getSerializer().toByteArray(record.getRecordVersion()));

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
		OMerkleTreeNode treeNode = new OMerkleTreeNode(createDbLookup(), 1);

		for (ONodeId i = ONodeId.MAX_VALUE; i.compareTo(ONodeId.MAX_VALUE.subtract(ONodeId.valueOf(66))) >= 0; i = i.subtract(ONodeId
						.valueOf(1))) {
			int childPos = OMerkleTreeNode.childIndex(0, i);
			ONodeId startKey = OMerkleTreeNode.startNodeId(1, childPos, ONodeId.valueOf(0));

      final ODocument doc = new ODocument();
      doc.setIdentity(new ORecordId(1, new OClusterPositionNodeId(i)));
      doc.field("value", i + "");
      treeNode.addRecord(1, startKey, doc);
		}

		for (ONodeId i = ONodeId.MAX_VALUE; i.compareTo(ONodeId.MAX_VALUE.subtract(ONodeId.valueOf(66))) >= 0; i = i.subtract(ONodeId
						.valueOf(1))) {
      final ODocument doc = db.load(new ORecordId(1, new OClusterPositionNodeId(i)));
      Assert.assertEquals(doc.field("value"), i + "");
    }

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

			ODocument record = db.load(new ORecordId(1, new OClusterPositionNodeId(id)));

			buffer.put(id.chunksToByteArray());
			buffer.put(record.getRecordVersion().getSerializer().toByteArray(record.getRecordVersion()));
		}

		buffer.rewind();

		MessageDigest sha = MessageDigest.getInstance("SHA-1");
		sha.update(buffer);

		Assert.assertEquals(child.getHash(), sha.digest());

		final ByteBuffer lastChildBuffer = ByteBuffer.allocate(OMerkleTreeNode.LEAF_BUFFER_ENTRY_SIZE * 3);

		ODocument record = db.load(new ORecordId(1, new OClusterPositionNodeId(ONodeId.MAX_VALUE.subtract(ONodeId.valueOf(66)))));

		lastChildBuffer.put(((OClusterPositionNodeId) record.getIdentity().getClusterPosition()).getNodeId().chunksToByteArray());
		lastChildBuffer.put(record.getRecordVersion().getSerializer().toByteArray(record.getRecordVersion()));

		record = db.load(new ORecordId(1, new OClusterPositionNodeId(ONodeId.MAX_VALUE.subtract(ONodeId.valueOf(65)))));
		lastChildBuffer.put(((OClusterPositionNodeId) record.getIdentity().getClusterPosition()).getNodeId().chunksToByteArray());
		lastChildBuffer.put(record.getRecordVersion().getSerializer().toByteArray(record.getRecordVersion()));

		record = db.load(new ORecordId(1, new OClusterPositionNodeId(ONodeId.MAX_VALUE.subtract(ONodeId.valueOf(64)))));

		lastChildBuffer.put(((OClusterPositionNodeId) record.getIdentity().getClusterPosition()).getNodeId().chunksToByteArray());
		lastChildBuffer.put(record.getRecordVersion().getSerializer().toByteArray(record.getRecordVersion()));

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
    OMerkleTreeNode treeNode = new OMerkleTreeNode(createDbLookup(), 1);
    for (int i = 0; i < 2; i++) {
      final ODocument doc = new ODocument();
      doc.setIdentity(convertTORID(i));
      doc.field("value", i + "");
      treeNode.addRecord(1, ONodeId.valueOf(0), doc);
    }

    for (int i = 1024; i < 1089; i++) {
      final ODocument doc = new ODocument();
      doc.setIdentity(convertTORID(i));
      doc.field("value", i + "");
      treeNode.addRecord(1, ONodeId.valueOf(0), doc);
    }
      
		for (long i = 0; i < 2; i++)
			Assert.assertEquals(((ODocument) db.load(convertTORID(i))).field("value"), i + "");

		for (long i = 1024; i < 1089; i++)
			Assert.assertEquals(((ODocument) db.load(convertTORID(i))).field("value"), i + "");

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
			final ODocument record = db.load(convertTORID(1024L + n));

			buffer.put(((OClusterPositionNodeId) record.getIdentity().getClusterPosition()).getNodeId().chunksToByteArray());
			buffer.put(record.getRecordVersion().getSerializer().toByteArray(record.getRecordVersion()));
		}

		buffer.rewind();

		MessageDigest sha = MessageDigest.getInstance("SHA-1");
		sha.update(buffer);

		Assert.assertEquals(child.getHash(), sha.digest());

		ODocument record = db.load(convertTORID(1088L));
		final ByteBuffer lastChildBuffer = ByteBuffer.allocate(OMerkleTreeNode.LEAF_BUFFER_ENTRY_SIZE);

		lastChildBuffer.put(ONodeId.valueOf(1088).chunksToByteArray());
		lastChildBuffer.put(record.getRecordVersion().getSerializer().toByteArray(record.getRecordVersion()));

		lastChildBuffer.rewind();

		MessageDigest lastChildSHA = MessageDigest.getInstance("SHA-1");
		lastChildSHA.update(lastChildBuffer);

		Assert.assertEquals(parent.getChild(17).getHash(), lastChildSHA.digest());

		MessageDigest firstNodeSHA = MessageDigest.getInstance("SHA-1");

		ByteBuffer firstNodeBuffer = ByteBuffer.allocate(OMerkleTreeNode.LEAF_BUFFER_ENTRY_SIZE * 2);

		record = db.load(convertTORID(0));

		firstNodeBuffer.put(((OClusterPositionNodeId) record.getIdentity().getClusterPosition()).getNodeId().chunksToByteArray());
		firstNodeBuffer.put(record.getRecordVersion().getSerializer().toByteArray(record.getRecordVersion()));

		record = db.load(convertTORID(1));

		firstNodeBuffer.put(((OClusterPositionNodeId) record.getIdentity().getClusterPosition()).getNodeId().chunksToByteArray());
		firstNodeBuffer.put(record.getRecordVersion().getSerializer().toByteArray(record.getRecordVersion()));

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
		final OMerkleTreeNode testTreeNode = new OMerkleTreeNode(createDbLookup(), 1);
		for (int i = 0; i < 69; i++) {
      final ODocument doc = new ODocument();
      doc.setIdentity(convertTORID(i));
      doc.field("value", i + "");

			testTreeNode.addRecord(1, ONodeId.valueOf(0), doc);
    }

		for (long i = 66; i < 69; i++) {
			final ODocument record = db.load(convertTORID(i));
			testTreeNode.deleteRecord(1, ONodeId.valueOf(0), record.getIdentity(), record.getRecordVersion());
		}

		for (long i = 0; i < 66; i++)
			Assert.assertEquals(((ODocument) db.load(convertTORID(i))).field("value"), i + "");


		final OMerkleTreeNode sampleTreeNode = new OMerkleTreeNode(createDb2Lookup(), 1);

		for (ODocument doc : db.browseCluster(db.getClusterNameById(CLUSTER_ID), new OClusterPositionNodeId(ONodeId.MIN_VALUE), new OClusterPositionNodeId(ONodeId.MAX_VALUE), true)) {
      final ODocument copy = doc.copy();
      copy.setDirty();
      sampleTreeNode.updateReplica(1, ONodeId.valueOf(0), doc.getIdentity(), copy);
    }

		compareNodes(sampleTreeNode, testTreeNode);
	}


	public void testAdd69KeysRemove60() {
		OMerkleTreeNode testTreeNode = new OMerkleTreeNode(createDbLookup(), 1);
    for (int i = 0; i < 69; i++) {
      final ODocument doc = new ODocument();
      doc.setIdentity(convertTORID(i));
      doc.field("value", i + "");

      testTreeNode.addRecord(1, ONodeId.ZERO, doc);
    }

		for (long i = 9; i < 69; i++) {
			final ORecordInternal<?> record = db.load(convertTORID(i));

			testTreeNode.deleteRecord(1, ONodeId.valueOf(0), convertTORID(i), record.getRecordVersion());
		}

		for (long i = 0; i < 9; i++)
			Assert.assertEquals(((ODocument) db.load(convertTORID(i))).field("value"), i + "");

		OMerkleTreeNode sampleTreeNode = new OMerkleTreeNode(createDb2Lookup(), 1);

		for (ODocument doc : db.browseCluster(db.getClusterNameById(CLUSTER_ID), new OClusterPositionNodeId(ONodeId.MIN_VALUE), new OClusterPositionNodeId(ONodeId.MAX_VALUE), true))
			sampleTreeNode.updateReplica(1, ONodeId.valueOf(0), doc.getIdentity(), doc);

		compareNodes(sampleTreeNode, testTreeNode);
	}

	public void testAdd67KeysToNext1024NodeRemove60() throws Exception {
		OMerkleTreeNode testTreeNode = new OMerkleTreeNode(createDbLookup(), 1);
		for (int i = 0; i < 2; i++){
      final ODocument doc = new ODocument();
      doc.setIdentity(convertTORID(i));
      doc.field("value", i + "");

      testTreeNode.addRecord(1, ONodeId.ZERO, doc);
    }

		for (int i = 1024; i < 1089; i++){
      final ODocument doc = new ODocument();
      doc.setIdentity(convertTORID(i));
      doc.field("value", i + "");

      testTreeNode.addRecord(1, ONodeId.ZERO, doc);
    }

		for (long i = 1024; i < 1084; i++) {
			final ORecordInternal<?> record = db.load(convertTORID(i));

			testTreeNode.deleteRecord(1, ONodeId.valueOf(0), convertTORID(i), record.getRecordVersion());
		}

		for (long i = 0; i < 2; i++)
			Assert.assertEquals(((ODocument) db.load(convertTORID(i))).field("value"), i + "");

		for (long i = 1084; i < 1089; i++)
			Assert.assertEquals(((ODocument) db.load(convertTORID(i))).field("value"), i + "");

		OMerkleTreeNode sampleTreeNode = new OMerkleTreeNode(createDb2Lookup(), 1);

		for (ODocument doc : db.browseCluster(db.getClusterNameById(CLUSTER_ID), new OClusterPositionNodeId(ONodeId.MIN_VALUE), new OClusterPositionNodeId(ONodeId.MAX_VALUE), true))
			sampleTreeNode.updateReplica(1, ONodeId.valueOf(0), doc.getIdentity(), doc);

		compareNodes(sampleTreeNode, testTreeNode);
	}

  public void testAdd67KeysToEndRemove60() throws Exception {
		OMerkleTreeNode testTreeNode = new OMerkleTreeNode(createDbLookup(), 1);

		for (ONodeId i = ONodeId.MAX_VALUE; i.compareTo(ONodeId.MAX_VALUE.subtract(ONodeId.valueOf(66))) >= 0; i = i
						.subtract(ONodeId.ONE)) {
			int childPos = OMerkleTreeNode.childIndex(0, i);
			ONodeId startKey = OMerkleTreeNode.startNodeId(1, childPos, ONodeId.ZERO);

      final ODocument doc = new ODocument();
      doc.setIdentity(new ORecordId(1, new OClusterPositionNodeId(i)));
      doc.field("value", i + "");

      testTreeNode.addRecord(1, startKey, doc);
		}

		for (ONodeId i = ONodeId.MAX_VALUE.subtract(ONodeId.valueOf(66)); i.compareTo(ONodeId.MAX_VALUE.subtract(ONodeId.valueOf(6))) < 0; i = i
						.add(ONodeId.ONE)) {
			final int childPos = OMerkleTreeNode.childIndex(0, i);
			final ONodeId startKey = OMerkleTreeNode.startNodeId(1, childPos, ONodeId.ZERO);

			final ORecordInternal<?> record = db.load(new ORecordId(1, new OClusterPositionNodeId(i)));

			testTreeNode.deleteRecord(1, startKey, record.getIdentity(), record.getRecordVersion());
		}

		for (ONodeId i = ONodeId.MAX_VALUE.subtract(ONodeId.valueOf(6)); i.compareTo(ONodeId.ZERO) > 0; i = i.add(ONodeId.ONE))
			Assert.assertEquals(((ODocument) db.load(new ORecordId(1, new OClusterPositionNodeId(i)))).field("value"), i + "");

		OMerkleTreeNode sampleTreeNode = new OMerkleTreeNode(createDb2Lookup(), 1);

		for (ODocument doc : db.browseCluster(db.getClusterNameById(CLUSTER_ID), new OClusterPositionNodeId(ONodeId.MIN_VALUE), new OClusterPositionNodeId(ONodeId.MAX_VALUE), true)) {
			final int childPos = OMerkleTreeNode.childIndex(0, ((OClusterPositionNodeId) doc.getIdentity().getClusterPosition()).getNodeId());
			ONodeId startKey = OMerkleTreeNode.startNodeId(1, childPos, ONodeId.ZERO);

			sampleTreeNode.updateReplica(1, startKey, doc.getIdentity(), doc);
		}

		compareNodes(sampleTreeNode, testTreeNode);
	}

	public void testAdd69KeysCleanOut3() {
		OMerkleTreeNode testTreeNode = new OMerkleTreeNode(createDbLookup(), 1);
		for (int i = 0; i < 69; i++) {
      final ODocument doc = new ODocument();
      doc.setIdentity(convertTORID(i));
      doc.field("value", i + "");

			testTreeNode.addRecord(1, ONodeId.ZERO, doc);
    }

		for (long i = 68; i < 69; i++) {
			final ORecordInternal<?> record = db.load(convertTORID(i));
			testTreeNode.cleanOutRecord(1, ONodeId.ZERO, record.getIdentity(), record.getRecordVersion());
		}

		for (long i = 0; i < 66; i++)
			Assert.assertEquals(((ODocument) db.load(convertTORID(i))).field("value"), i + "");

		OMerkleTreeNode sampleTreeNode = new OMerkleTreeNode(createDb2Lookup(), 1);

		for (ODocument doc : db.browseCluster(db.getClusterNameById(CLUSTER_ID), new OClusterPositionNodeId(ONodeId.MIN_VALUE), new OClusterPositionNodeId(ONodeId.MAX_VALUE), true)) {
			if (doc.getRecordVersion().isTombstone())
				Assert.fail();

			sampleTreeNode.updateReplica(1, ONodeId.ZERO, doc.getIdentity(), doc);
		}

		compareNodes(sampleTreeNode, testTreeNode);
	}

	public void testAdd69KeysCleanOut60() {
		OMerkleTreeNode testTreeNode = new OMerkleTreeNode(createDbLookup(), 1);
		for (int i = 0; i < 69; i++) {
      final ODocument doc = new ODocument();
      doc.setIdentity(convertTORID(i));
      doc.field("value", i + "");

      testTreeNode.addRecord(1, ONodeId.ZERO, doc);
    }

		for (long i = 64; i < 69; i++) {
			final ORecordInternal<?> record = db.load(convertTORID(i));

			testTreeNode.cleanOutRecord(1, ONodeId.ZERO, record.getIdentity(), record.getRecordVersion());
		}

		for (long i = 0; i < 64; i++)
			Assert.assertEquals(((ODocument) db.load(convertTORID(i))).field("value"), i + "");

		OMerkleTreeNode sampleTreeNode = new OMerkleTreeNode(createDb2Lookup(), 1);

		for (ODocument doc : db.browseCluster(db.getClusterNameById(CLUSTER_ID), new OClusterPositionNodeId(ONodeId.MIN_VALUE), new OClusterPositionNodeId(ONodeId.MAX_VALUE), true)) {
			if (doc.getRecordVersion().isTombstone())
				Assert.fail();

			sampleTreeNode.updateReplica(1, ONodeId.ZERO, doc.getIdentity(), doc);
		}

		compareNodes(sampleTreeNode, testTreeNode);
	}

	public void testAdd67KeysToNext1024NodeCleanOut60() throws Exception {
		OMerkleTreeNode testTreeNode = new OMerkleTreeNode(createDbLookup(), 1);
		for (int i = 0; i < 2; i++) {
      final ODocument doc = new ODocument();
      doc.setIdentity(convertTORID(i));
      doc.field("value", i + "");

      testTreeNode.addRecord(1, ONodeId.ZERO, doc);
    }

		for (int i = 1024; i < 1089; i++) {
      final ODocument doc = new ODocument();
      doc.setIdentity(convertTORID(i));
      doc.field("value", i + "");

      testTreeNode.addRecord(1, ONodeId.ZERO, doc);
    }

		for (long i = 1024; i < 1084; i++) {
			final ORecordInternal<?> record = db.load(convertTORID(i));

			testTreeNode.cleanOutRecord(1, ONodeId.ZERO, record.getIdentity(), record.getRecordVersion());
		}

		for (long i = 0; i < 2; i++) {
      Assert.assertEquals(((ODocument) db.load(convertTORID(i))).field("value"), i + "");
    }

		for (long i = 1084; i < 1089; i++) {
      Assert.assertEquals(((ODocument) db.load(convertTORID(i))).field("value"), i + "");
    }

		OMerkleTreeNode sampleTreeNode = new OMerkleTreeNode(createDb2Lookup(), 1);

		for (ODocument doc : db.browseCluster(db.getClusterNameById(CLUSTER_ID), new OClusterPositionNodeId(ONodeId.MIN_VALUE), new OClusterPositionNodeId(ONodeId.MAX_VALUE), true)) {
			if (doc.getRecordVersion().isTombstone())
				Assert.fail();

			sampleTreeNode.updateReplica(1, ONodeId.ZERO, doc.getIdentity(), doc);
		}

		compareNodes(sampleTreeNode, testTreeNode);
	}

	public void testAdd67KeysToEndCleanOut60() throws Exception {
		OMerkleTreeNode testTreeNode = new OMerkleTreeNode(createDbLookup(), 1);

		for (ONodeId i = ONodeId.MAX_VALUE; i.compareTo(ONodeId.MAX_VALUE.subtract(ONodeId.valueOf(66))) >= 0; i = i
						.subtract(ONodeId.ONE)) {
			int childPos = OMerkleTreeNode.childIndex(0, i);
			ONodeId startKey = OMerkleTreeNode.startNodeId(1, childPos, ONodeId.ZERO);

      final ODocument doc = new ODocument();
      doc.setIdentity(new ORecordId(1, new OClusterPositionNodeId(i)));
      doc.field("value", i + "");

      testTreeNode.addRecord(1, startKey, doc);
		}

		for (ONodeId i = ONodeId.MAX_VALUE.subtract(ONodeId.valueOf(66)); i.compareTo(ONodeId.MAX_VALUE.subtract(ONodeId.valueOf(62))) < 0; i = i
						.add(ONodeId.ONE)) {
			final int childPos = OMerkleTreeNode.childIndex(0, i);
			final ONodeId startKey = OMerkleTreeNode.startNodeId(1, childPos, ONodeId.ZERO);

			final ORecordInternal<?> record = db.load(new ORecordId(1, new OClusterPositionNodeId(i)));

			testTreeNode.cleanOutRecord(1, startKey, record.getIdentity(), record.getRecordVersion());
		}

		for (ONodeId i = ONodeId.MAX_VALUE.subtract(ONodeId.valueOf(60)); i.compareTo(ONodeId.ZERO) > 0; i = i.add(ONodeId.ONE))
			Assert.assertEquals(((ODocument) db.load(new ORecordId(1, new OClusterPositionNodeId(i)))).field("value"), i + "");

		OMerkleTreeNode sampleTreeNode = new OMerkleTreeNode(createDb2Lookup(), 1);

		for (ODocument doc : db.browseCluster(db.getClusterNameById(CLUSTER_ID), new OClusterPositionNodeId(ONodeId.MIN_VALUE), new OClusterPositionNodeId(ONodeId.MAX_VALUE), true)) {
			int childPos = OMerkleTreeNode.childIndex(0, ((OClusterPositionNodeId) doc.getIdentity().getClusterPosition()).getNodeId());
			ONodeId startKey = OMerkleTreeNode.startNodeId(1, childPos, ONodeId.ZERO);

			if (doc.getRecordVersion().isTombstone())
				Assert.fail();

			sampleTreeNode.updateReplica(1, startKey, doc.getIdentity(), doc);
		}

		compareNodes(sampleTreeNode, testTreeNode);
	}

	public void testAddOneKeyUpdateOneKey() throws Exception {
		OMerkleTreeNode treeNode = new OMerkleTreeNode(createDbLookup(), 1);

    final ODocument doc = new ODocument();
    doc.setIdentity(convertTORID(130));
    doc.field("value", "130");
		treeNode.addRecord(1, ONodeId.ZERO, doc);

		ODocument record = db.load(convertTORID(130L));
    record.field("value", "150");

		treeNode.updateRecord(1, ONodeId.ZERO, record.getIdentity(), record.getRecordVersion(), record);

		record = db.load(convertTORID(130L));
		Assert.assertEquals(record.field("value"), "150");
		Assert.assertEquals(record.getRecordVersion().getCounter(), 1);

		MessageDigest sha = MessageDigest.getInstance("SHA-1");

		final ByteBuffer byteBuffer = ByteBuffer.allocate(OMerkleTreeNode.LEAF_BUFFER_ENTRY_SIZE);
		byteBuffer.put(ONodeId.valueOf(130L).chunksToByteArray());
		byteBuffer.put(record.getRecordVersion().getSerializer().toByteArray(record.getRecordVersion()));

		byteBuffer.rewind();
		sha.update(byteBuffer);

		Assert.assertEquals(treeNode.getHash(), sha.digest());
		Assert.assertEquals(treeNode.getRecordsCount(), 1);
	}

	public void testAddOneKeyUpdateOneKeyTwoTimes() throws Exception {
		OMerkleTreeNode treeNode = new OMerkleTreeNode(createDbLookup(), 1);

    final ODocument doc = new ODocument();
    doc.setIdentity(convertTORID(130));
    doc.field("value", "130");
		treeNode.addRecord(1, ONodeId.ZERO, doc);

		ODocument record = db.load(convertTORID(130L));

    record.field("value", "150");
    treeNode.updateRecord(1, ONodeId.ZERO, convertTORID(130), record.getRecordVersion(), record);
    record.field("value", "160");
    treeNode.updateRecord(1, ONodeId.ZERO, convertTORID(130), record.getRecordVersion(), record);

    record = db.load(convertTORID(130L));
    Assert.assertEquals(record.field("value"), "160");
		Assert.assertEquals(record.getRecordVersion().getCounter(), 2);

		MessageDigest sha = MessageDigest.getInstance("SHA-1");

		final ByteBuffer byteBuffer = ByteBuffer.allocate(OMerkleTreeNode.LEAF_BUFFER_ENTRY_SIZE);
		byteBuffer.put(ONodeId.valueOf(130L).chunksToByteArray());
		byteBuffer.put(record.getRecordVersion().getSerializer().toByteArray(record.getRecordVersion()));

		byteBuffer.rewind();
		sha.update(byteBuffer);

		Assert.assertEquals(treeNode.getHash(), sha.digest());
		Assert.assertEquals(treeNode.getRecordsCount(), 1);
	}

	public void testAddOneKeyUpdateOneKeySecondTimeWithWrongVersion() throws Exception {
		OMerkleTreeNode treeNode = new OMerkleTreeNode(createDbLookup(), 1);

    final ODocument doc = new ODocument();
    doc.setIdentity(convertTORID(130));
    doc.field("value", "130");
		treeNode.addRecord(1, ONodeId.ZERO, doc);

		ODocument record = db.load(convertTORID(130L));

    record.field("value", "150");
    treeNode.updateRecord(1, ONodeId.ZERO, record.getIdentity(), record.getRecordVersion(), record);

		try {
			final ORecordVersion version = new ODistributedVersion(34);

      record.getRecordVersion().copyFrom(version);
      record.field("value", "160");
      treeNode.updateRecord(1, ONodeId.ZERO, convertTORID(130), version, record);
			Assert.fail();
		} catch (OConcurrentModificationException e) {
		}

		record = db.load(convertTORID(130L));
		Assert.assertEquals(record.field("value"), "150");
		Assert.assertEquals(record.getRecordVersion().getCounter(), 1);

		MessageDigest sha = MessageDigest.getInstance("SHA-1");

		final ByteBuffer byteBuffer = ByteBuffer.allocate(OMerkleTreeNode.LEAF_BUFFER_ENTRY_SIZE);
		byteBuffer.put(ONodeId.valueOf(130).chunksToByteArray());
		byteBuffer.put(record.getRecordVersion().getSerializer().toByteArray(record.getRecordVersion()));

		byteBuffer.rewind();
		sha.update(byteBuffer);

		Assert.assertEquals(treeNode.getHash(), sha.digest());
		Assert.assertEquals(treeNode.getRecordsCount(), 1);
	}

	public void testAdd69KeysUpdate3() {
		OMerkleTreeNode testTreeNode = new OMerkleTreeNode(createDbLookup(), 1);
		for (int i = 0; i < 69; i++) {
      final ODocument doc = new ODocument();
      doc.setIdentity(convertTORID(i));
      doc.field("value", i + "");

      testTreeNode.addRecord(1, ONodeId.ZERO, doc);
    }

		for (long i = 68; i < 69; i++) {
			final ODocument record = db.load(convertTORID(i));
      record.field("value", "11" + i);

      testTreeNode.updateRecord(1, ONodeId.ZERO, record.getIdentity(), record.getRecordVersion(), record);
		}

		for (long i = 0; i < 66; i++)
			Assert.assertEquals(((ODocument) db.load(convertTORID(i))).field("value"), i + "");

		for (long i = 68; i < 69; i++)
			Assert.assertEquals(((ODocument) db.load(convertTORID(i))).field("value"), "11" + i);

		OMerkleTreeNode sampleTreeNode = new OMerkleTreeNode(createDb2Lookup(), 1);

		for (ODocument doc : db.browseCluster(db.getClusterNameById(CLUSTER_ID), new OClusterPositionNodeId(ONodeId.MIN_VALUE), new OClusterPositionNodeId(ONodeId.MAX_VALUE), true))
			sampleTreeNode.updateReplica(1, ONodeId.ZERO, doc.getIdentity(), doc);

		compareNodes(sampleTreeNode, testTreeNode);
	}

	public void testAdd69KeysUpdate60() {
		OMerkleTreeNode testTreeNode = new OMerkleTreeNode(createDbLookup(), 1);
		for (int i = 0; i < 69; i++) {
      final ODocument doc = new ODocument();
      doc.setIdentity(convertTORID(i));
      doc.field("value", i + "");
      
      testTreeNode.addRecord(1, ONodeId.ZERO, doc);
    }

		for (long i = 64; i < 69; i++) {
			final ODocument record = db.load(convertTORID(i));
      record.field("value", "11" + i);

			testTreeNode.updateRecord(1, ONodeId.ZERO, convertTORID(i), record.getRecordVersion(), record);
		}

		for (long i = 0; i < 64; i++)
			Assert.assertEquals(((ODocument) db.load(convertTORID(i))).field("value"), i + "");

		for (long i = 64; i < 69; i++)
			Assert.assertEquals(((ODocument) db.load(convertTORID(i))).field("value"), "11" + i);

		OMerkleTreeNode sampleTreeNode = new OMerkleTreeNode(createDb2Lookup(), 1);

		for (ODocument doc : db.browseCluster(db.getClusterNameById(CLUSTER_ID), new OClusterPositionNodeId(ONodeId.MIN_VALUE), new OClusterPositionNodeId(ONodeId.MAX_VALUE), true))
			sampleTreeNode.updateReplica(1, ONodeId.ZERO, doc.getIdentity(), doc);

		compareNodes(sampleTreeNode, testTreeNode);
	}

	public void testAdd67KeysToNext1024NodeUpdate60() throws Exception {
		OMerkleTreeNode testTreeNode = new OMerkleTreeNode(createDbLookup(), 1);
		for (int i = 0; i < 2; i++) {
      final ODocument doc = new ODocument();
      doc.setIdentity(convertTORID(i));
      doc.field("value", i + "");

      testTreeNode.addRecord(1, ONodeId.ZERO, doc);
    }

		for (int i = 1024; i < 1089; i++) {
      final ODocument doc = new ODocument();
      doc.setIdentity(convertTORID(i));
      doc.field("value", i + "");

      testTreeNode.addRecord(1, ONodeId.ZERO, doc);
    }

		for (long i = 1024; i < 1084; i++) {
			final ODocument record = db.load(convertTORID(i));
      record.field("value", "11" + i);

			testTreeNode.updateRecord(1, ONodeId.ZERO, convertTORID(i), record.getRecordVersion(), record);
		}

		for (long i = 0; i < 2; i++)
			Assert.assertEquals(((ODocument) db.load(convertTORID(i))).field("value"), i + "");

		for (long i = 1024; i < 1084; i++)
			Assert.assertEquals(((ODocument) db.load(convertTORID(i))).field("value"), "11" + i);

		for (long i = 1084; i < 1089; i++)
			Assert.assertEquals(((ODocument) db.load(convertTORID(i))).field("value"), i + "");

		OMerkleTreeNode sampleTreeNode = new OMerkleTreeNode(createDb2Lookup(), 1);

		for (ODocument doc : db.browseCluster(db.getClusterNameById(CLUSTER_ID), new OClusterPositionNodeId(ONodeId.MIN_VALUE), new OClusterPositionNodeId(ONodeId.MAX_VALUE), true)) {
			sampleTreeNode.updateReplica(1, ONodeId.ZERO, doc.getIdentity(), doc);
		}

		compareNodes(sampleTreeNode, testTreeNode);
	}

	public void testAdd67KeysToEndUpdate60() throws Exception {
		OMerkleTreeNode testTreeNode = new OMerkleTreeNode(createDbLookup(), 1);

		for (ONodeId i = ONodeId.MAX_VALUE; i.compareTo(ONodeId.MAX_VALUE.subtract(ONodeId.valueOf(66))) >= 0; i = i
						.subtract(ONodeId.ONE)) {
			int childPos = OMerkleTreeNode.childIndex(0, i);
			ONodeId startKey = OMerkleTreeNode.startNodeId(1, childPos, ONodeId.ZERO);

      final ODocument doc = new ODocument();
      doc.setIdentity(new ORecordId(1, new OClusterPositionNodeId(i)));
      doc.field("value", i + "");

			testTreeNode.addRecord(1, startKey, doc);
		}

		for (ONodeId i = ONodeId.MAX_VALUE.subtract(ONodeId.valueOf(66)); i.compareTo(ONodeId.MAX_VALUE.subtract(ONodeId.valueOf(6))) < 0; i = i
						.add(ONodeId.ONE)) {
			final int childPos = OMerkleTreeNode.childIndex(0, i);
			final ONodeId startKey = OMerkleTreeNode.startNodeId(1, childPos, ONodeId.ZERO);

			final ORecordInternal<?> record = db.load(new ORecordId(1, new OClusterPositionNodeId(i)));

			testTreeNode.deleteRecord(1, startKey, new ORecordId(1, new OClusterPositionNodeId(i)), record.getRecordVersion());
		}

		for (ONodeId i = ONodeId.MAX_VALUE.subtract(ONodeId.valueOf(6)); i.compareTo(ONodeId.ZERO) > 0; i = i.add(ONodeId.ONE))
			Assert.assertEquals(((ODocument) db.load(new ORecordId(1, new OClusterPositionNodeId(i)))).field("value"), i + "");

		OMerkleTreeNode sampleTreeNode = new OMerkleTreeNode(createDb2Lookup(), 1);

		for (ODocument doc : db.browseCluster(db.getClusterNameById(CLUSTER_ID), new OClusterPositionNodeId(ONodeId.MIN_VALUE), new OClusterPositionNodeId(ONodeId.MAX_VALUE), true)) {
			int childPos = OMerkleTreeNode.childIndex(0, ((OClusterPositionNodeId) doc.getIdentity().getClusterPosition()).getNodeId());
			ONodeId startKey = OMerkleTreeNode.startNodeId(1, childPos, ONodeId.ZERO);

			sampleTreeNode.updateReplica(1, startKey, doc.getIdentity(), doc);
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

	private ORecordId convertTORID(long i) {
		return new ORecordId(1, new OClusterPositionNodeId(ONodeId.valueOf(i)));
	}

  private ODHTDatabaseLookupImpl createDbLookup() {
    return new ODHTDatabaseLookupImpl("memory:mercleTreeTest", "admin", "admin");
  }

  private ODHTDatabaseLookupImpl createDb2Lookup() {
    return new ODHTDatabaseLookupImpl("memory:mercleTreeTestSecondaryDb", "admin", "admin");
  }
}
