package com.orientechnologies.orient.server.distributed;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * @author Andrey Lomakin
 * @since 10.09.12
 */
@Test
public class MerkleTreeNodeTest {
	public void testAddOneKey() throws Exception {
		NavigableMap<Long, String> map = new TreeMap<Long, String>();

		OMerkleTreeNode treeNode = new OMerkleTreeNode(map);
		treeNode.addData(1, 0, 130, "130");

		Assert.assertEquals(map.get(130L), "130");

		MessageDigest sha = MessageDigest.getInstance("SHA-1");

		final ByteBuffer byteBuffer = ByteBuffer.allocate(8).putLong(130);
		byteBuffer.rewind();
		sha.update(byteBuffer);

		Assert.assertEquals(treeNode.getHash(), sha.digest());
		Assert.assertEquals(treeNode.getKeyCount(), 1);
	}

	public void testAdd66Keys() throws Exception {
		NavigableMap<Long, String> map = new TreeMap<Long, String>();

		OMerkleTreeNode treeNode = new OMerkleTreeNode(map);
		for (int i = 0; i < 66; i++)
			treeNode.addData(1, 0, i, i + "");

		for (long i = 0; i < 66; i++)
			Assert.assertEquals(map.get(i), i + "");


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
			Assert.assertEquals(treeNode.getChild(i).getKeyCount(), 16);

		Assert.assertEquals(treeNode.getChild(4).getKeyCount(), 2);

		for (int i = 0; i < 4; i++) {
			final ByteBuffer buffer = ByteBuffer.allocate(16 * 8);

			OMerkleTreeNode child = treeNode.getChild(i);
			for (int n = 0; n < 16; n++)
				buffer.putLong(i * 16 + n);

			buffer.rewind();

			MessageDigest sha = MessageDigest.getInstance("SHA-1");
			sha.update(buffer);

			Assert.assertEquals(child.getHash(), sha.digest());
		}

		final ByteBuffer lastChildBuffer = ByteBuffer.allocate(16);
		lastChildBuffer.putLong(64);
		lastChildBuffer.putLong(65);

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
		NavigableMap<Long, String> map = new TreeMap<Long, String>();

		OMerkleTreeNode treeNode = new OMerkleTreeNode(map);

		for (long i = Long.MAX_VALUE; i >= Long.MAX_VALUE - 66; i--) {
			long childPos = OMerkleTreeNode.childIndex(0, i);
			long startKey = OMerkleTreeNode.startNodeKey(1, childPos, 0);

			treeNode.addData(1, startKey, i, i + "");
		}

		for (long i = Long.MAX_VALUE; i >= Long.MAX_VALUE - 66; i--)
			Assert.assertEquals(map.get(i), i + "");

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
			Assert.assertEquals(treeNode.getChild(i).getKeyCount(), 16);

		Assert.assertEquals(treeNode.getChild(59).getKeyCount(), 3);

		for (int i = 63; i >= 60; i--) {
			final ByteBuffer buffer = ByteBuffer.allocate(16 * 8);

			OMerkleTreeNode child = treeNode.getChild(i);
			for (int n = 15; n >= 0; n--)
				buffer.putLong(Long.MAX_VALUE - n - 16 * (63 - i));

			buffer.rewind();

			MessageDigest sha = MessageDigest.getInstance("SHA-1");
			sha.update(buffer);

			Assert.assertEquals(child.getHash(), sha.digest());
		}

		final ByteBuffer lastChildBuffer = ByteBuffer.allocate(24);
		lastChildBuffer.putLong(Long.MAX_VALUE - 66);
		lastChildBuffer.putLong(Long.MAX_VALUE - 65);
		lastChildBuffer.putLong(Long.MAX_VALUE - 64);

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
		NavigableMap<Long, String> map = new TreeMap<Long, String>();

		OMerkleTreeNode treeNode = new OMerkleTreeNode(map);
		for (int i = 0; i < 2; i++)
			treeNode.addData(1, 0, i, i + "");

		for (int i = 1024; i < 1089; i++)
			treeNode.addData(1, 0, i, i + "");

		for (long i = 0; i < 2; i++)
			Assert.assertEquals(map.get(i), i + "");

		for (long i = 1024; i < 1089; i++)
			Assert.assertEquals(map.get(i), i + "");

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
		Assert.assertEquals(parent.getChild(0).getKeyCount(), 2);

		treeNode = treeNode.getChild(1);

		Assert.assertTrue(!treeNode.isLeaf());

		for (int i = 0; i < 4; i++)
			Assert.assertEquals(treeNode.getChild(i).getKeyCount(), 16);

		Assert.assertEquals(treeNode.getChild(4).getKeyCount(), 1);

		for (int i = 0; i < 4; i++) {
			final ByteBuffer buffer = ByteBuffer.allocate(16 * 8);

			OMerkleTreeNode child = treeNode.getChild(i);
			for (int n = 0; n < 16; n++)
				buffer.putLong(1024 + i * 16 + n);

			buffer.rewind();

			MessageDigest sha = MessageDigest.getInstance("SHA-1");
			sha.update(buffer);

			Assert.assertEquals(child.getHash(), sha.digest());
		}

		final ByteBuffer lastChildBuffer = ByteBuffer.allocate(8);
		lastChildBuffer.putLong(1088);

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
		ByteBuffer firstNodeBuffer = ByteBuffer.allocate(16);

		firstNodeBuffer.putLong(0);
		firstNodeBuffer.putLong(1);

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
		NavigableMap<Long, String> mapOne = new TreeMap<Long, String>();

		OMerkleTreeNode testTreeNode = new OMerkleTreeNode(mapOne);
		for (int i = 0; i < 69; i++)
			testTreeNode.addData(1, 0, i, i + "");

		for (int i = 66; i < 69; i++)
			testTreeNode.deleteData(1, 0, i);

		for (long i = 0; i < 66; i++)
			Assert.assertEquals(mapOne.get(i), i + "");

		NavigableMap<Long, String> mapTwo = new TreeMap<Long, String>();
		OMerkleTreeNode sampleTreeNode = new OMerkleTreeNode(mapTwo);

		for (int i = 0; i < 66; i++)
			sampleTreeNode.addData(1, 0, i, i + "");

		compareNodes(sampleTreeNode, testTreeNode);
	}

	public void testAdd69KeysRemove60() {
		NavigableMap<Long, String> mapOne = new TreeMap<Long, String>();

		OMerkleTreeNode testTreeNode = new OMerkleTreeNode(mapOne);
		for (int i = 0; i < 69; i++)
			testTreeNode.addData(1, 0, i, i + "");

		for (int i = 64; i < 69; i++)
			testTreeNode.deleteData(1, 0, i);

		for (long i = 0; i < 64; i++)
			Assert.assertEquals(mapOne.get(i), i + "");

		NavigableMap<Long, String> mapTwo = new TreeMap<Long, String>();
		OMerkleTreeNode sampleTreeNode = new OMerkleTreeNode(mapTwo);

		for (int i = 0; i < 64; i++)
			sampleTreeNode.addData(1, 0, i, i + "");

		compareNodes(sampleTreeNode, testTreeNode);
	}

	public void testAdd67KeysToNext1024NodeRemove60() throws Exception {
		NavigableMap<Long, String> map = new TreeMap<Long, String>();

		OMerkleTreeNode testTreeNode = new OMerkleTreeNode(map);
		for (int i = 0; i < 2; i++)
			testTreeNode.addData(1, 0, i, i + "");

		for (int i = 1024; i < 1089; i++)
			testTreeNode.addData(1, 0, i, i + "");

		for (int i = 1024; i < 1084; i++)
			testTreeNode.deleteData(1, 0, i);

		for (long i = 0; i < 2; i++)
			Assert.assertEquals(map.get(i), i + "");

		for (long i = 1084; i < 1089; i++)
			Assert.assertEquals(map.get(i), i + "");

		NavigableMap<Long, String> mapTwo = new TreeMap<Long, String>();
		OMerkleTreeNode sampleTreeNode = new OMerkleTreeNode(mapTwo);

		for (int i = 0; i < 2; i++)
			sampleTreeNode.addData(1, 0, i, i + "");

		for (long i = 1084; i < 1089; i++)
			sampleTreeNode.addData(1, 0, i, i + "");

		compareNodes(sampleTreeNode, testTreeNode);
	}

	public void testAdd67KeysToEndRemove60() throws Exception {
		NavigableMap<Long, String> map = new TreeMap<Long, String>();

		OMerkleTreeNode testTreeNode = new OMerkleTreeNode(map);

		for (long i = Long.MAX_VALUE; i >= Long.MAX_VALUE - 66; i--) {
			long childPos = OMerkleTreeNode.childIndex(0, i);
			long startKey = OMerkleTreeNode.startNodeKey(1, childPos, 0);

			testTreeNode.addData(1, startKey, i, i + "");
		}

		for (long i = Long.MAX_VALUE - 66; i < Long.MAX_VALUE - 60; i++) {
			long childPos = OMerkleTreeNode.childIndex(0, i);
			long startKey = OMerkleTreeNode.startNodeKey(1, childPos, 0);

			testTreeNode.deleteData(1, startKey, i);
		}

		for (long i = Long.MAX_VALUE - 60; i > 0; i++)
			Assert.assertEquals(map.get(i), i + "");

		NavigableMap<Long, String> mapTwo = new TreeMap<Long, String>();
		OMerkleTreeNode sampleTreeNode = new OMerkleTreeNode(mapTwo);

		for (long i = Long.MAX_VALUE - 60; i > 0; i++) {
			long childPos = OMerkleTreeNode.childIndex(0, i);
			long startKey = OMerkleTreeNode.startNodeKey(1, childPos, 0);

			sampleTreeNode.addData(1, startKey, i, i + "");
		}

		compareNodes(sampleTreeNode, testTreeNode);
	}

	private void compareNodes(OMerkleTreeNode nodeOne, OMerkleTreeNode nodeTwo) {
		Assert.assertEquals(nodeOne.getHash(), nodeTwo.getHash());
		Assert.assertEquals(nodeOne.isLeaf(), nodeTwo.isLeaf());
		Assert.assertEquals(nodeOne.getKeyCount(), nodeTwo.getKeyCount());

		if (!nodeOne.isLeaf()) {
			for (int i = 0; i < 64; i++) {
				final OMerkleTreeNode childOne = nodeOne.getChild(i);
				final OMerkleTreeNode childTwo = nodeTwo.getChild(i);

				compareNodes(childOne, childTwo);
			}
		}
	}
}