package com.orientechnologies.orient.server.distributed;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.version.ODistributedVersion;
import com.orientechnologies.orient.core.version.ORecordVersion;
import com.orientechnologies.orient.server.distributed.merkletree.OMerkleTreeNode;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.id.OClusterPositionNodeId;
import com.orientechnologies.orient.core.id.ONodeId;
import com.orientechnologies.orient.core.id.ORID;

/**
 * @author Andrey Lomakin
 * @since 10.09.12
 */
@Test
public class MerkleTreeNodeTest {
	@BeforeMethod
	public void setUp() {
		OGlobalConfiguration.DB_USE_DISTRIBUTED_VERSION.setValue(true);
	}

	public void testAddOneKey() throws Exception {
		NavigableMap<ORID, Record> map = new TreeMap<ORID, Record>();

		OMerkleTreeNode treeNode = new OMerkleTreeNode(map, 1);
		treeNode.addRecord(1, ONodeId.valueOf(0), new ORecordId(1, new OClusterPositionNodeId(ONodeId.valueOf(130))), "130");

		Record record = map.get(convertTORID(130L));
		Assert.assertEquals(record.getData(), "130");
		Assert.assertEquals(record.getShortVersion(), 0);

		MessageDigest sha = MessageDigest.getInstance("SHA-1");

		final ByteBuffer byteBuffer = ByteBuffer.allocate(OMerkleTreeNode.LEAF_BUFFER_ENTRY_SIZE);
		byteBuffer.put(((OClusterPositionNodeId) record.getId().getClusterPosition()).getNodeId().chunksToByteArray());
		byteBuffer.put(record.getVersion().getSerializer().toByteArray(record.getVersion()));

		byteBuffer.rewind();
		sha.update(byteBuffer);

		Assert.assertEquals(treeNode.getHash(), sha.digest());
		Assert.assertEquals(treeNode.getRecordsCount(), 1);
	}

	public void testAdd66Keys() throws Exception {
		NavigableMap<ORID, Record> map = new TreeMap<ORID, Record>();

		OMerkleTreeNode treeNode = new OMerkleTreeNode(map, 1);
		for (int i = 0; i < 66; i++)
			treeNode.addRecord(1, ONodeId.valueOf(0), convertTORID(i), i + "");

		for (long i = 0; i < 66; i++)
			Assert.assertEquals(map.get(convertTORID(i)).getData(), i + "");

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
			Record record = map.get(convertTORID(n));

			buffer.put(((OClusterPositionNodeId) record.getId().getClusterPosition()).getNodeId().chunksToByteArray());
			buffer.put(record.getVersion().getSerializer().toByteArray(record.getVersion()));
		}

		buffer.limit(buffer.position());
		buffer.rewind();

		MessageDigest sha = MessageDigest.getInstance("SHA-1");
		sha.update(buffer);

		Assert.assertEquals(child.getHash(), sha.digest());

		final ByteBuffer lastChildBuffer = ByteBuffer.allocate(OMerkleTreeNode.LEAF_BUFFER_ENTRY_SIZE * 2);
		Record record;

		record = map.get(convertTORID(64L));
		lastChildBuffer.put(((OClusterPositionNodeId) record.getId().getClusterPosition()).getNodeId().chunksToByteArray());
		lastChildBuffer.put(record.getVersion().getSerializer().toByteArray(record.getVersion()));

		record = map.get(convertTORID(65L));
		lastChildBuffer.put(((OClusterPositionNodeId) record.getId().getClusterPosition()).getNodeId().chunksToByteArray());
		lastChildBuffer.put(record.getVersion().getSerializer().toByteArray(record.getVersion()));

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
		NavigableMap<ORID, Record> map = new TreeMap<ORID, Record>();

		OMerkleTreeNode treeNode = new OMerkleTreeNode(map, 1);

		for (ONodeId i = ONodeId.MAX_VALUE; i.compareTo(ONodeId.MAX_VALUE.subtract(ONodeId.valueOf(66))) >= 0; i = i.subtract(ONodeId
						.valueOf(1))) {
			int childPos = OMerkleTreeNode.childIndex(0, i);
			ONodeId startKey = OMerkleTreeNode.startNodeId(1, childPos, ONodeId.valueOf(0));

			treeNode.addRecord(1, startKey, new ORecordId(1, new OClusterPositionNodeId(i)), i + "");
		}

		for (ONodeId i = ONodeId.MAX_VALUE; i.compareTo(ONodeId.MAX_VALUE.subtract(ONodeId.valueOf(66))) >= 0; i = i.subtract(ONodeId
						.valueOf(1)))
			Assert.assertEquals(map.get(new ORecordId(1, new OClusterPositionNodeId(i))).getData(), i + "");

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

			Record record = map.get(new ORecordId(1, new OClusterPositionNodeId(id)));

			buffer.put(id.chunksToByteArray());
			buffer.put(record.getVersion().getSerializer().toByteArray(record.getVersion()));
		}

		buffer.rewind();

		MessageDigest sha = MessageDigest.getInstance("SHA-1");
		sha.update(buffer);

		Assert.assertEquals(child.getHash(), sha.digest());

		final ByteBuffer lastChildBuffer = ByteBuffer.allocate(OMerkleTreeNode.LEAF_BUFFER_ENTRY_SIZE * 3);

		Record record = map.get(new ORecordId(1, new OClusterPositionNodeId(ONodeId.MAX_VALUE.subtract(ONodeId.valueOf(66)))));

		lastChildBuffer.put(((OClusterPositionNodeId) record.getId().getClusterPosition()).getNodeId().chunksToByteArray());
		lastChildBuffer.put(record.getVersion().getSerializer().toByteArray(record.getVersion()));

		record = map.get(new ORecordId(1, new OClusterPositionNodeId(ONodeId.MAX_VALUE.subtract(ONodeId.valueOf(65)))));
		lastChildBuffer.put(((OClusterPositionNodeId) record.getId().getClusterPosition()).getNodeId().chunksToByteArray());
		lastChildBuffer.put(record.getVersion().getSerializer().toByteArray(record.getVersion()));

		record = map.get(new ORecordId(1, new OClusterPositionNodeId(ONodeId.MAX_VALUE.subtract(ONodeId.valueOf(64)))));

		lastChildBuffer.put(((OClusterPositionNodeId) record.getId().getClusterPosition()).getNodeId().chunksToByteArray());
		lastChildBuffer.put(record.getVersion().getSerializer().toByteArray(record.getVersion()));

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
		NavigableMap<ORID, Record> map = new TreeMap<ORID, Record>();

		OMerkleTreeNode treeNode = new OMerkleTreeNode(map, 1);
		for (int i = 0; i < 2; i++)
			treeNode.addRecord(1, ONodeId.valueOf(0), convertTORID(i), i + "");

		for (int i = 1024; i < 1089; i++)
			treeNode.addRecord(1, ONodeId.valueOf(0), convertTORID(i), i + "");

		for (long i = 0; i < 2; i++)
			Assert.assertEquals(map.get(convertTORID(i)).getData(), i + "");

		for (long i = 1024; i < 1089; i++)
			Assert.assertEquals(map.get(convertTORID(i)).getData(), i + "");

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
			final Record record = map.get(convertTORID(1024L + n));

			buffer.put(((OClusterPositionNodeId) record.getId().getClusterPosition()).getNodeId().chunksToByteArray());
			buffer.put(record.getVersion().getSerializer().toByteArray(record.getVersion()));
		}

		buffer.rewind();

		MessageDigest sha = MessageDigest.getInstance("SHA-1");
		sha.update(buffer);

		Assert.assertEquals(child.getHash(), sha.digest());

		Record record = map.get(convertTORID(1088L));
		final ByteBuffer lastChildBuffer = ByteBuffer.allocate(OMerkleTreeNode.LEAF_BUFFER_ENTRY_SIZE);

		lastChildBuffer.put(ONodeId.valueOf(1088).chunksToByteArray());
		lastChildBuffer.put(record.getVersion().getSerializer().toByteArray(record.getVersion()));

		lastChildBuffer.rewind();

		MessageDigest lastChildSHA = MessageDigest.getInstance("SHA-1");
		lastChildSHA.update(lastChildBuffer);

		Assert.assertEquals(parent.getChild(17).getHash(), lastChildSHA.digest());

		MessageDigest firstNodeSHA = MessageDigest.getInstance("SHA-1");

		ByteBuffer firstNodeBuffer = ByteBuffer.allocate(OMerkleTreeNode.LEAF_BUFFER_ENTRY_SIZE * 2);

		record = map.get(convertTORID(0));

		firstNodeBuffer.put(((OClusterPositionNodeId) record.getId().getClusterPosition()).getNodeId().chunksToByteArray());
		firstNodeBuffer.put(record.getVersion().getSerializer().toByteArray(record.getVersion()));

		record = map.get(convertTORID(1));

		firstNodeBuffer.put(((OClusterPositionNodeId) record.getId().getClusterPosition()).getNodeId().chunksToByteArray());
		firstNodeBuffer.put(record.getVersion().getSerializer().toByteArray(record.getVersion()));

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
		NavigableMap<ORID, Record> mapOne = new TreeMap<ORID, Record>();

		OMerkleTreeNode testTreeNode = new OMerkleTreeNode(mapOne, 1);
		for (int i = 0; i < 69; i++)
			testTreeNode.addRecord(1, ONodeId.valueOf(0), convertTORID(i), i + "");

		for (long i = 68; i < 69; i++) {
			final Record record = mapOne.get(convertTORID(i));
			testTreeNode.deleteRecord(1, ONodeId.valueOf(0), record.getId(), record.getVersion());
		}

		for (long i = 0; i < 66; i++)
			Assert.assertEquals(mapOne.get(convertTORID(i)).getData(), i + "");

		NavigableMap<ORID, Record> mapTwo = new TreeMap<ORID, Record>();
		OMerkleTreeNode sampleTreeNode = new OMerkleTreeNode(mapTwo, 1);

		for (Map.Entry<ORID, Record> entry : mapOne.entrySet())
			sampleTreeNode.updateReplica(1, ONodeId.valueOf(0), entry.getKey(), entry.getValue());

		compareNodes(sampleTreeNode, testTreeNode);
	}

	public void testAdd69KeysRemove60() {
		NavigableMap<ORID, Record> mapOne = new TreeMap<ORID, Record>();

		OMerkleTreeNode testTreeNode = new OMerkleTreeNode(mapOne, 1);
		for (int i = 0; i < 69; i++)
			testTreeNode.addRecord(1, ONodeId.valueOf(0), convertTORID(i), i + "");

		for (long i = 64; i < 69; i++) {
			final Record record = mapOne.get(convertTORID(i));

			testTreeNode.deleteRecord(1, ONodeId.valueOf(0), convertTORID(i), record.getVersion());
		}

		for (long i = 0; i < 64; i++)
			Assert.assertEquals(mapOne.get(convertTORID(i)).getData(), i + "");

		NavigableMap<ORID, Record> mapTwo = new TreeMap<ORID, Record>();
		OMerkleTreeNode sampleTreeNode = new OMerkleTreeNode(mapTwo, 1);

		for (Map.Entry<ORID, Record> entry : mapOne.entrySet())
			sampleTreeNode.updateReplica(1, ONodeId.valueOf(0), entry.getKey(), entry.getValue());

		compareNodes(sampleTreeNode, testTreeNode);
	}

	public void testAdd67KeysToNext1024NodeRemove60() throws Exception {
		NavigableMap<ORID, Record> map = new TreeMap<ORID, Record>();

		OMerkleTreeNode testTreeNode = new OMerkleTreeNode(map, 1);
		for (int i = 0; i < 2; i++)
			testTreeNode.addRecord(1, ONodeId.valueOf(0), convertTORID(i), i + "");

		for (int i = 1024; i < 1089; i++)
			testTreeNode.addRecord(1, ONodeId.valueOf(0), convertTORID(i), i + "");

		for (long i = 1024; i < 1084; i++) {
			final Record record = map.get(convertTORID(i));

			testTreeNode.deleteRecord(1, ONodeId.valueOf(0), convertTORID(i), record.getVersion());
		}

		for (long i = 0; i < 2; i++)
			Assert.assertEquals(map.get(convertTORID(i)).getData(), i + "");

		for (long i = 1084; i < 1089; i++)
			Assert.assertEquals(map.get(convertTORID(i)).getData(), i + "");

		NavigableMap<ORID, Record> mapTwo = new TreeMap<ORID, Record>();
		OMerkleTreeNode sampleTreeNode = new OMerkleTreeNode(mapTwo, 1);

		for (Map.Entry<ORID, Record> entry : map.entrySet())
			sampleTreeNode.updateReplica(1, ONodeId.valueOf(0), entry.getKey(), entry.getValue());

		compareNodes(sampleTreeNode, testTreeNode);
	}

	public void testAdd67KeysToEndRemove60() throws Exception {
		NavigableMap<ORID, Record> map = new TreeMap<ORID, Record>();

		OMerkleTreeNode testTreeNode = new OMerkleTreeNode(map, 1);

		for (ONodeId i = ONodeId.MAX_VALUE; i.compareTo(ONodeId.MAX_VALUE.subtract(ONodeId.valueOf(66))) >= 0; i = i
						.subtract(ONodeId.ONE)) {
			int childPos = OMerkleTreeNode.childIndex(0, i);
			ONodeId startKey = OMerkleTreeNode.startNodeId(1, childPos, ONodeId.ZERO);

			testTreeNode.addRecord(1, startKey, new ORecordId(1, new OClusterPositionNodeId(i)), i + "");
		}

		for (ONodeId i = ONodeId.MAX_VALUE.subtract(ONodeId.valueOf(66)); i.compareTo(ONodeId.MAX_VALUE.subtract(ONodeId.valueOf(6))) < 0; i = i
						.add(ONodeId.ONE)) {
			final int childPos = OMerkleTreeNode.childIndex(0, i);
			final ONodeId startKey = OMerkleTreeNode.startNodeId(1, childPos, ONodeId.ZERO);

			final Record record = map.get(new ORecordId(1, new OClusterPositionNodeId(i)));

			testTreeNode.deleteRecord(1, startKey, record.getId(), record.getVersion());
		}

		for (ONodeId i = ONodeId.MAX_VALUE.subtract(ONodeId.valueOf(6)); i.compareTo(ONodeId.ZERO) > 0; i = i.add(ONodeId.ONE))
			Assert.assertEquals(map.get(new ORecordId(1, new OClusterPositionNodeId(i))).getData(), i + "");

		NavigableMap<ORID, Record> mapTwo = new TreeMap<ORID, Record>();
		OMerkleTreeNode sampleTreeNode = new OMerkleTreeNode(mapTwo, 1);

		for (Map.Entry<ORID, Record> entry : map.entrySet()) {
			final int childPos = OMerkleTreeNode.childIndex(0, ((OClusterPositionNodeId) entry.getKey().getClusterPosition()).getNodeId());
			ONodeId startKey = OMerkleTreeNode.startNodeId(1, childPos, ONodeId.ZERO);

			sampleTreeNode.updateReplica(1, startKey, entry.getKey(), entry.getValue());
		}

		compareNodes(sampleTreeNode, testTreeNode);
	}

	public void testAdd69KeysCleanOut3() {
		NavigableMap<ORID, Record> mapOne = new TreeMap<ORID, Record>();

		OMerkleTreeNode testTreeNode = new OMerkleTreeNode(mapOne, 1);
		for (int i = 0; i < 69; i++)
			testTreeNode.addRecord(1, ONodeId.ZERO, convertTORID(i), i + "");

		for (long i = 68; i < 69; i++) {
			final Record record = mapOne.get(convertTORID(i));
			testTreeNode.cleanOutRecord(1, ONodeId.ZERO, record.getId(), record.getVersion());
		}

		for (long i = 0; i < 66; i++)
			Assert.assertEquals(mapOne.get(convertTORID(i)).getData(), i + "");

		NavigableMap<ORID, Record> mapTwo = new TreeMap<ORID, Record>();
		OMerkleTreeNode sampleTreeNode = new OMerkleTreeNode(mapTwo, 1);

		for (Map.Entry<ORID, Record> entry : mapOne.entrySet()) {
			if (entry.getValue().isTombstone())
				Assert.fail();

			sampleTreeNode.updateReplica(1, ONodeId.ZERO, entry.getKey(), entry.getValue());
		}

		compareNodes(sampleTreeNode, testTreeNode);
	}

	public void testAdd69KeysCleanOut60() {
		NavigableMap<ORID, Record> mapOne = new TreeMap<ORID, Record>();

		OMerkleTreeNode testTreeNode = new OMerkleTreeNode(mapOne, 1);
		for (int i = 0; i < 69; i++)
			testTreeNode.addRecord(1, ONodeId.ZERO, convertTORID(i), i + "");

		for (long i = 64; i < 69; i++) {
			final Record record = mapOne.get(convertTORID(i));

			testTreeNode.cleanOutRecord(1, ONodeId.ZERO, record.getId(), record.getVersion());
		}

		for (long i = 0; i < 64; i++)
			Assert.assertEquals(mapOne.get(convertTORID(i)).getData(), i + "");

		NavigableMap<ORID, Record> mapTwo = new TreeMap<ORID, Record>();
		OMerkleTreeNode sampleTreeNode = new OMerkleTreeNode(mapTwo, 1);

		for (Map.Entry<ORID, Record> entry : mapOne.entrySet()) {
			if (entry.getValue().isTombstone())
				Assert.fail();

			sampleTreeNode.updateReplica(1, ONodeId.ZERO, entry.getKey(), entry.getValue());
		}

		compareNodes(sampleTreeNode, testTreeNode);
	}

	public void testAdd67KeysToNext1024NodeCleanOut60() throws Exception {
		NavigableMap<ORID, Record> map = new TreeMap<ORID, Record>();

		OMerkleTreeNode testTreeNode = new OMerkleTreeNode(map, 1);
		for (int i = 0; i < 2; i++)
			testTreeNode.addRecord(1, ONodeId.ZERO, convertTORID(i), i + "");

		for (int i = 1024; i < 1089; i++)
			testTreeNode.addRecord(1, ONodeId.ZERO, convertTORID(i), i + "");

		for (long i = 1024; i < 1084; i++) {
			final Record record = map.get(convertTORID(i));

			testTreeNode.cleanOutRecord(1, ONodeId.ZERO, record.getId(), record.getVersion());
		}

		for (long i = 0; i < 2; i++)
			Assert.assertEquals(map.get(convertTORID(i)).getData(), i + "");

		for (long i = 1084; i < 1089; i++)
			Assert.assertEquals(map.get(convertTORID(i)).getData(), i + "");

		NavigableMap<ORID, Record> mapTwo = new TreeMap<ORID, Record>();
		OMerkleTreeNode sampleTreeNode = new OMerkleTreeNode(mapTwo, 1);

		for (Map.Entry<ORID, Record> entry : map.entrySet()) {
			if (entry.getValue().isTombstone())
				Assert.fail();

			sampleTreeNode.updateReplica(1, ONodeId.ZERO, entry.getKey(), entry.getValue());
		}

		compareNodes(sampleTreeNode, testTreeNode);
	}

	public void testAdd67KeysToEndCleanOut60() throws Exception {
		NavigableMap<ORID, Record> map = new TreeMap<ORID, Record>();

		OMerkleTreeNode testTreeNode = new OMerkleTreeNode(map, 1);

		for (ONodeId i = ONodeId.MAX_VALUE; i.compareTo(ONodeId.MAX_VALUE.subtract(ONodeId.valueOf(66))) >= 0; i = i
						.subtract(ONodeId.ONE)) {
			int childPos = OMerkleTreeNode.childIndex(0, i);
			ONodeId startKey = OMerkleTreeNode.startNodeId(1, childPos, ONodeId.ZERO);

			testTreeNode.addRecord(1, startKey, new ORecordId(1, new OClusterPositionNodeId(i)), i + "");
		}

		for (ONodeId i = ONodeId.MAX_VALUE.subtract(ONodeId.valueOf(66)); i.compareTo(ONodeId.MAX_VALUE.subtract(ONodeId.valueOf(62))) < 0; i = i
						.add(ONodeId.ONE)) {
			final int childPos = OMerkleTreeNode.childIndex(0, i);
			final ONodeId startKey = OMerkleTreeNode.startNodeId(1, childPos, ONodeId.ZERO);

			final Record record = map.get(new ORecordId(1, new OClusterPositionNodeId(i)));

			testTreeNode.cleanOutRecord(1, startKey, record.getId(), record.getVersion());
		}

		for (ONodeId i = ONodeId.MAX_VALUE.subtract(ONodeId.valueOf(60)); i.compareTo(ONodeId.ZERO) > 0; i = i.add(ONodeId.ONE))
			Assert.assertEquals(map.get(new ORecordId(1, new OClusterPositionNodeId(i))).getData(), i + "");

		NavigableMap<ORID, Record> mapTwo = new TreeMap<ORID, Record>();
		OMerkleTreeNode sampleTreeNode = new OMerkleTreeNode(mapTwo, 1);

		for (Map.Entry<ORID, Record> entry : map.entrySet()) {
			int childPos = OMerkleTreeNode.childIndex(0, ((OClusterPositionNodeId) entry.getKey().getClusterPosition()).getNodeId());
			ONodeId startKey = OMerkleTreeNode.startNodeId(1, childPos, ONodeId.ZERO);

			if (entry.getValue().isTombstone())
				Assert.fail();

			sampleTreeNode.updateReplica(1, startKey, entry.getKey(), entry.getValue());
		}

		compareNodes(sampleTreeNode, testTreeNode);
	}

	public void testAddOneKeyUpdateOneKey() throws Exception {
		NavigableMap<ORID, Record> map = new TreeMap<ORID, Record>();

		OMerkleTreeNode treeNode = new OMerkleTreeNode(map, 1);
		treeNode.addRecord(1, ONodeId.ZERO, convertTORID(130), "130");

		Record record = map.get(convertTORID(130L));

		treeNode.updateRecord(1, ONodeId.ZERO, record.getId(), record.getVersion(), "150");

		record = map.get(convertTORID(130L));
		Assert.assertEquals(record.getData(), "150");
		Assert.assertEquals(record.getShortVersion(), 1);

		MessageDigest sha = MessageDigest.getInstance("SHA-1");

		final ByteBuffer byteBuffer = ByteBuffer.allocate(OMerkleTreeNode.LEAF_BUFFER_ENTRY_SIZE);
		byteBuffer.put(ONodeId.valueOf(130L).chunksToByteArray());
		byteBuffer.put(record.getVersion().getSerializer().toByteArray(record.getVersion()));

		byteBuffer.rewind();
		sha.update(byteBuffer);

		Assert.assertEquals(treeNode.getHash(), sha.digest());
		Assert.assertEquals(treeNode.getRecordsCount(), 1);
	}

	public void testAddOneKeyUpdateOneKeyTwoTimes() throws Exception {
		NavigableMap<ORID, Record> map = new TreeMap<ORID, Record>();

		OMerkleTreeNode treeNode = new OMerkleTreeNode(map, 1);
		treeNode.addRecord(1, ONodeId.ZERO, convertTORID(130), "130");

		final Record record = map.get(convertTORID(130L));

		treeNode.updateRecord(1, ONodeId.ZERO, convertTORID(130), record.getVersion(), "150");
		treeNode.updateRecord(1, ONodeId.ZERO, convertTORID(130), record.getVersion(), "160");

		Assert.assertEquals(record.getData(), "160");
		Assert.assertEquals(record.getShortVersion(), 2);

		MessageDigest sha = MessageDigest.getInstance("SHA-1");

		final ByteBuffer byteBuffer = ByteBuffer.allocate(OMerkleTreeNode.LEAF_BUFFER_ENTRY_SIZE);
		byteBuffer.put(ONodeId.valueOf(130L).chunksToByteArray());
		byteBuffer.put(record.getVersion().getSerializer().toByteArray(record.getVersion()));

		byteBuffer.rewind();
		sha.update(byteBuffer);

		Assert.assertEquals(treeNode.getHash(), sha.digest());
		Assert.assertEquals(treeNode.getRecordsCount(), 1);
	}

	public void testAddOneKeyUpdateOneKeySecondTimeWithWrongVersion() throws Exception {
		NavigableMap<ORID, Record> map = new TreeMap<ORID, Record>();

		OMerkleTreeNode treeNode = new OMerkleTreeNode(map, 1);
		treeNode.addRecord(1, ONodeId.ZERO, convertTORID(130), "130");

		Record record = map.get(convertTORID(130L));

		treeNode.updateRecord(1, ONodeId.ZERO, record.getId(), record.getVersion(), "150");

		try {
			final ORecordVersion version = new ODistributedVersion(34);

			treeNode.updateRecord(1, ONodeId.ZERO, convertTORID(130), version, "160");
			Assert.fail();
		} catch (OConcurrentModificationException e) {
		}

		record = map.get(convertTORID(130L));
		Assert.assertEquals(record.getData(), "150");
		Assert.assertEquals(record.getShortVersion(), 1);

		MessageDigest sha = MessageDigest.getInstance("SHA-1");

		final ByteBuffer byteBuffer = ByteBuffer.allocate(OMerkleTreeNode.LEAF_BUFFER_ENTRY_SIZE);
		byteBuffer.put(ONodeId.valueOf(130).chunksToByteArray());
		byteBuffer.put(record.getVersion().getSerializer().toByteArray(record.getVersion()));

		byteBuffer.rewind();
		sha.update(byteBuffer);

		Assert.assertEquals(treeNode.getHash(), sha.digest());
		Assert.assertEquals(treeNode.getRecordsCount(), 1);
	}

	public void testAdd69KeysUpdate3() {
		NavigableMap<ORID, Record> mapOne = new TreeMap<ORID, Record>();

		OMerkleTreeNode testTreeNode = new OMerkleTreeNode(mapOne, 1);
		for (int i = 0; i < 69; i++)
			testTreeNode.addRecord(1, ONodeId.ZERO, convertTORID(i), i + "");

		for (long i = 68; i < 69; i++) {
			final Record record = mapOne.get(convertTORID(i));

			testTreeNode.updateRecord(1, ONodeId.ZERO, record.getId(), record.getVersion(), "11" + i);
		}

		for (long i = 0; i < 66; i++)
			Assert.assertEquals(mapOne.get(convertTORID(i)).getData(), i + "");

		for (long i = 68; i < 69; i++)
			Assert.assertEquals(mapOne.get(convertTORID(i)).getData(), "11" + i);

		NavigableMap<ORID, Record> mapTwo = new TreeMap<ORID, Record>();
		OMerkleTreeNode sampleTreeNode = new OMerkleTreeNode(mapTwo, 1);

		for (Map.Entry<ORID, Record> entry : mapOne.entrySet())
			sampleTreeNode.updateReplica(1, ONodeId.ZERO, entry.getKey(), entry.getValue());

		compareNodes(sampleTreeNode, testTreeNode);
	}

	public void testAdd69KeysUpdate60() {
		NavigableMap<ORID, Record> mapOne = new TreeMap<ORID, Record>();

		OMerkleTreeNode testTreeNode = new OMerkleTreeNode(mapOne, 1);
		for (int i = 0; i < 69; i++)
			testTreeNode.addRecord(1, ONodeId.ZERO, convertTORID(i), i + "");

		for (long i = 64; i < 69; i++) {
			final Record record = mapOne.get(convertTORID(i));

			testTreeNode.updateRecord(1, ONodeId.ZERO, convertTORID(i), record.getVersion(), "11" + i);
		}

		for (long i = 0; i < 64; i++)
			Assert.assertEquals(mapOne.get(convertTORID(i)).getData(), i + "");

		for (long i = 64; i < 69; i++)
			Assert.assertEquals(mapOne.get(convertTORID(i)).getData(), "11" + i);

		NavigableMap<ORID, Record> mapTwo = new TreeMap<ORID, Record>();
		OMerkleTreeNode sampleTreeNode = new OMerkleTreeNode(mapTwo, 1);

		for (Map.Entry<ORID, Record> entry : mapOne.entrySet())
			sampleTreeNode.updateReplica(1, ONodeId.ZERO, entry.getKey(), entry.getValue());

		compareNodes(sampleTreeNode, testTreeNode);
	}

	public void testAdd67KeysToNext1024NodeUpdate60() throws Exception {
		NavigableMap<ORID, Record> map = new TreeMap<ORID, Record>();

		OMerkleTreeNode testTreeNode = new OMerkleTreeNode(map, 1);
		for (int i = 0; i < 2; i++)
			testTreeNode.addRecord(1, ONodeId.ZERO, convertTORID(i), i + "");

		for (int i = 1024; i < 1089; i++)
			testTreeNode.addRecord(1, ONodeId.ZERO, convertTORID(i), i + "");

		for (long i = 1024; i < 1084; i++) {
			final Record record = map.get(convertTORID(i));

			testTreeNode.updateRecord(1, ONodeId.ZERO, convertTORID(i), record.getVersion(), "11" + i);
		}

		for (long i = 0; i < 2; i++)
			Assert.assertEquals(map.get(convertTORID(i)).getData(), i + "");

		for (long i = 1024; i < 1084; i++)
			Assert.assertEquals(map.get(convertTORID(i)).getData(), "11" + i);

		for (long i = 1084; i < 1089; i++)
			Assert.assertEquals(map.get(convertTORID(i)).getData(), i + "");

		NavigableMap<ORID, Record> mapTwo = new TreeMap<ORID, Record>();
		OMerkleTreeNode sampleTreeNode = new OMerkleTreeNode(mapTwo, 1);

		for (Map.Entry<ORID, Record> entry : map.entrySet()) {
			sampleTreeNode.updateReplica(1, ONodeId.ZERO, entry.getKey(), entry.getValue());
		}

		compareNodes(sampleTreeNode, testTreeNode);
	}

	public void testAdd67KeysToEndUpdate60() throws Exception {
		NavigableMap<ORID, Record> map = new TreeMap<ORID, Record>();

		OMerkleTreeNode testTreeNode = new OMerkleTreeNode(map, 1);

		for (ONodeId i = ONodeId.MAX_VALUE; i.compareTo(ONodeId.MAX_VALUE.subtract(ONodeId.valueOf(66))) >= 0; i = i
						.subtract(ONodeId.ONE)) {
			int childPos = OMerkleTreeNode.childIndex(0, i);
			ONodeId startKey = OMerkleTreeNode.startNodeId(1, childPos, ONodeId.ZERO);

			testTreeNode.addRecord(1, startKey, new ORecordId(1, new OClusterPositionNodeId(i)), i + "");
		}

		for (ONodeId i = ONodeId.MAX_VALUE.subtract(ONodeId.valueOf(66)); i.compareTo(ONodeId.MAX_VALUE.subtract(ONodeId.valueOf(6))) < 0; i = i
						.add(ONodeId.ONE)) {
			final int childPos = OMerkleTreeNode.childIndex(0, i);
			final ONodeId startKey = OMerkleTreeNode.startNodeId(1, childPos, ONodeId.ZERO);

			final Record record = map.get(new ORecordId(1, new OClusterPositionNodeId(i)));

			testTreeNode.deleteRecord(1, startKey, new ORecordId(1, new OClusterPositionNodeId(i)), record.getVersion());
		}

		for (ONodeId i = ONodeId.MAX_VALUE.subtract(ONodeId.valueOf(6)); i.compareTo(ONodeId.ZERO) > 0; i = i.add(ONodeId.ONE))
			Assert.assertEquals(map.get(new ORecordId(1, new OClusterPositionNodeId(i))).getData(), i + "");

		NavigableMap<ORID, Record> mapTwo = new TreeMap<ORID, Record>();
		OMerkleTreeNode sampleTreeNode = new OMerkleTreeNode(mapTwo, 1);

		for (Map.Entry<ORID, Record> entry : map.entrySet()) {
			int childPos = OMerkleTreeNode.childIndex(0, ((OClusterPositionNodeId) entry.getKey().getClusterPosition()).getNodeId());
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

	private ORID convertTORID(long i) {
		return new ORecordId(1, new OClusterPositionNodeId(ONodeId.valueOf(i)));
	}
}
