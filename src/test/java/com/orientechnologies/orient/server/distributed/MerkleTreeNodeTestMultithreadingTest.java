package com.orientechnologies.orient.server.distributed;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Andrey Lomakin
 * @since 13.09.12
 */
@Test(enabled = false)
public class MerkleTreeNodeTestMultiThreadingTest {
	public void testConcurrentModifications() throws Exception {
		final ExecutorService adderExecutorService = Executors.newCachedThreadPool(new AdderThreadFactory());
		final ExecutorService readerExecutorService = Executors.newCachedThreadPool(new ReaderThreadFactory());
		final ExecutorService deleterExecutorService = Executors.newCachedThreadPool(new DeleterThreadFactory());

		final NavigableMap<Long, String> db = new ConcurrentSkipListMap<Long, String>();
		final CountDownLatch trigger = new CountDownLatch(1);

		final OMerkleTreeNode treeNode = new OMerkleTreeNode(db);

		final List<Future<Void>> writerFutures = new ArrayList<Future<Void>>();
		final List<Future<Void>> readerFutures = new ArrayList<Future<Void>>();
		final List<Future<Void>> deleterFutures = new ArrayList<Future<Void>>();

		final AtomicBoolean testIsFinished = new AtomicBoolean(false);

		final int interval = 500000;

		for (int i = 0; i < 5; i++)
			writerFutures.add(adderExecutorService.submit(new ConcurrentAdder(treeNode, trigger, i * interval, interval)));

		for (int i = 0; i < 2; i++)
			readerFutures.add(readerExecutorService.submit(new ConcurrentReader(treeNode, trigger, testIsFinished)));

		deleterFutures.add(deleterExecutorService.submit(new ConcurrentDeleter(treeNode,
						trigger, interval * 5, testIsFinished)));

		trigger.countDown();

		for (Future<Void> future : writerFutures)
			future.get();

		testIsFinished.set(true);

		for (Future<Void> future : readerFutures)
			future.get();

		for (Future<Void> future : deleterFutures)
			future.get();

		final NavigableMap<Long, String> dbTwo = new ConcurrentSkipListMap<Long, String>();
		final OMerkleTreeNode sampleTreeNode = new OMerkleTreeNode(dbTwo);

		System.out.println("Sample tree creation. DB size is : " + db.size());

		for (long key : db.keySet()) {
			long childPos = OMerkleTreeNode.childIndex(0, key);
			long startKey = OMerkleTreeNode.startNodeKey(1, childPos, 0);

			sampleTreeNode.addData(1, startKey, key, key + "");
		}

		System.out.println("Sample tree comparison.");

		compareNodes(sampleTreeNode, treeNode);
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

	private static class AdderThreadFactory implements ThreadFactory {
		final AtomicInteger counter = new AtomicInteger();

		public Thread newThread(Runnable r) {
			Thread thread = new Thread(r);
			thread.setName("Adder - " + counter.incrementAndGet());

			return thread;
		}
	}

	private static class ReaderThreadFactory implements ThreadFactory {
		final AtomicInteger counter = new AtomicInteger();

		public Thread newThread(Runnable r) {
			Thread thread = new Thread(r);
			thread.setName("Reader - " + counter.incrementAndGet());

			return thread;
		}
	}

	private static class DeleterThreadFactory implements ThreadFactory {
		final AtomicInteger counter = new AtomicInteger();

		public Thread newThread(Runnable r) {
			Thread thread = new Thread(r);
			thread.setName("Reader - " + counter.incrementAndGet());

			return thread;
		}
	}

	private final class ConcurrentDeleter implements Callable<Void> {
		private final OMerkleTreeNode node;
		private final CountDownLatch trigger;
		private final int interval;

		private final AtomicBoolean testIsFinished;

		private final Random random = new Random();

		private ConcurrentDeleter(OMerkleTreeNode node, CountDownLatch trigger, int interval, AtomicBoolean testIsFinished) {
			this.node = node;
			this.trigger = trigger;
			this.interval = interval;
			this.testIsFinished = testIsFinished;
		}

		public Void call() throws Exception {
			trigger.await();

			while (!testIsFinished.get()) {
				final int key = random.nextInt(interval);

				long childPos = OMerkleTreeNode.childIndex(0, key);
				long startKey = OMerkleTreeNode.startNodeKey(1, childPos, 0);

				node.deleteData(1, startKey, key);
			}

			return null;
		}
	}

	private final class ConcurrentAdder implements Callable<Void> {
		private final OMerkleTreeNode node;
		private final CountDownLatch trigger;
		private final long startPos;
		private final int interval;


		private ConcurrentAdder(OMerkleTreeNode node, CountDownLatch trigger, long startPos, int interval) {
			this.node = node;
			this.trigger = trigger;
			this.startPos = startPos;
			this.interval = interval;
		}

		public Void call() throws Exception {
			trigger.await();

			System.out.println(Thread.currentThread().getName() +
							":Insertions were started.");

			for (long i = startPos; i < startPos + interval; i++) {
				long childPos = OMerkleTreeNode.childIndex(0, i);
				long startKey = OMerkleTreeNode.startNodeKey(1, childPos, 0);

				node.addData(1, startKey, i, i + "");
				if ((i - startPos) % 10000 == 0)
					System.out.println(Thread.currentThread().getName() +
									":" + (i - startPos) + " records were inserted.");
			}

			System.out.println(Thread.currentThread().getName() + ":Insertions were finished.");

			return null;
		}
	}

	private static final class ConcurrentReader implements Callable<Void> {
		private final OMerkleTreeNode node;
		private final CountDownLatch trigger;
		private final AtomicBoolean testIsFinished;

		private ConcurrentReader(OMerkleTreeNode node, CountDownLatch trigger, AtomicBoolean testIsFinished) {
			this.node = node;
			this.trigger = trigger;
			this.testIsFinished = testIsFinished;
		}

		public Void call() throws Exception {
			trigger.await();

			System.out.println(Thread.currentThread().getName() + ":Reading is started.");

			while (!testIsFinished.get()) {
				readNode(node);
			}

			System.out.println(Thread.currentThread().getName() + ":Reading was finished.");

			return null;
		}

		private void readNode(OMerkleTreeNode node) {
			node.getHash();
			if (!node.isLeaf())
				for (int i = 0; i < 64; i++)
					if (testIsFinished.get()) {
						readNode(node.getChild(i));
					}

		}
	}
}