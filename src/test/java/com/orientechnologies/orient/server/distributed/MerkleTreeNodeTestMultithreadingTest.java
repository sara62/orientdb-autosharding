package com.orientechnologies.orient.server.distributed;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.orientechnologies.common.concur.lock.OLockManager;

/**
 * @author Andrey Lomakin
 * @since 13.09.12
 */
@Test
public class MerkleTreeNodeTestMultiThreadingTest {
  public void testConcurrentModifications() throws Exception {
    final ExecutorService adderExecutorService = Executors.newCachedThreadPool(new AdderThreadFactory());
    final ExecutorService readerExecutorService = Executors.newCachedThreadPool(new ReaderThreadFactory());
    final ExecutorService deleterExecutorService = Executors.newCachedThreadPool(new DeleterThreadFactory());

    final NavigableMap<Long, Record> db = new ConcurrentSkipListMap<Long, Record>();
    final CountDownLatch trigger = new CountDownLatch(1);

    final OMerkleTreeNode treeNode = new OMerkleTreeNode(db);

    final List<Future<Void>> writerFutures = new ArrayList<Future<Void>>();
    final List<Future<Void>> readerFutures = new ArrayList<Future<Void>>();
    final List<Future<Void>> deleterFutures = new ArrayList<Future<Void>>();

    final AtomicBoolean testIsFinished = new AtomicBoolean(false);

    final OLockManager<Long, Runnable> lockManager = new OLockManager<Long, Runnable>(true, 500);

    final int interval = 500000;

    for (int i = 0; i < 5; i++)
      writerFutures.add(adderExecutorService.submit(new ConcurrentAdder(treeNode, trigger, i * interval, interval, lockManager)));

    for (int i = 0; i < 2; i++)
      readerFutures.add(readerExecutorService.submit(new ConcurrentReader(treeNode, trigger, testIsFinished)));

    deleterFutures.add(deleterExecutorService.submit(new ConcurrentDeleter(treeNode, trigger, interval * 5, testIsFinished,
        lockManager, db)));

    trigger.countDown();

    for (Future<Void> future : writerFutures)
      future.get();

    testIsFinished.set(true);

    for (Future<Void> future : readerFutures)
      future.get();

    for (Future<Void> future : deleterFutures)
      future.get();

    final NavigableMap<Long, Record> dbTwo = new ConcurrentSkipListMap<Long, Record>();
    final OMerkleTreeNode sampleTreeNode = new OMerkleTreeNode(dbTwo);

    System.out.println("Sample tree creation. DB size is : " + db.size());

    for (Map.Entry<Long, Record> entry : db.entrySet()) {
      long childPos = OMerkleTreeNode.childIndex(0, entry.getKey());
      long startKey = OMerkleTreeNode.startNodeId(1, childPos, 0);

      sampleTreeNode.putReplica(1, startKey, entry.getKey(), entry.getValue());
    }

    System.out.println("Sample tree comparison.");

    compareNodes(sampleTreeNode, treeNode);
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
    private final OMerkleTreeNode              node;
    private final CountDownLatch               trigger;
    private final int                          interval;

    private final AtomicBoolean                testIsFinished;

    private final Random                       random = new Random();

    private final NavigableMap<Long, Record>   db;

    private final OLockManager<Long, Runnable> lockManager;

    private ConcurrentDeleter(OMerkleTreeNode node, CountDownLatch trigger, int interval, AtomicBoolean testIsFinished,
        OLockManager<Long, Runnable> lockManager, NavigableMap<Long, Record> db) {
      this.node = node;
      this.trigger = trigger;
      this.interval = interval;
      this.testIsFinished = testIsFinished;
      this.lockManager = lockManager;
      this.db = db;
    }

    public Void call() throws Exception {
      trigger.await();

      while (!testIsFinished.get()) {
        final long key = random.nextInt(interval);

        lockManager.acquireLock(Thread.currentThread(), key, OLockManager.LOCK.EXCLUSIVE);
        try {
          final Record record = db.get(key);

          if (record == null || record.isTombstone())
            continue;

          final long childPos = OMerkleTreeNode.childIndex(0, key);
          final long startKey = OMerkleTreeNode.startNodeId(1, childPos, 0);

          node.deleteRecord(1, startKey, key, record.getShortVersion());
        } finally {
          lockManager.releaseLock(Thread.currentThread(), key, OLockManager.LOCK.EXCLUSIVE);
        }
      }

      return null;
    }
  }

  private final class ConcurrentAdder implements Callable<Void> {
    private final OMerkleTreeNode              node;
    private final CountDownLatch               trigger;
    private final long                         startPos;
    private final int                          interval;
    private final OLockManager<Long, Runnable> lockManager;

    private ConcurrentAdder(OMerkleTreeNode node, CountDownLatch trigger, long startPos, int interval,
        OLockManager<Long, Runnable> lockManager) {
      this.node = node;
      this.trigger = trigger;
      this.startPos = startPos;
      this.interval = interval;
      this.lockManager = lockManager;
    }

    public Void call() throws Exception {
      trigger.await();

      System.out.println(Thread.currentThread().getName() + ":Insertions were started.");

      for (long i = startPos; i < startPos + interval; i++) {
        long childPos = OMerkleTreeNode.childIndex(0, i);
        long startKey = OMerkleTreeNode.startNodeId(1, childPos, 0);

        lockManager.acquireLock(Thread.currentThread(), i, OLockManager.LOCK.EXCLUSIVE);
        try {
          node.addRecord(1, startKey, i, i + "");
          if ((i - startPos) % 10000 == 0)
            System.out.println(Thread.currentThread().getName() + ":" + (i - startPos) + " records were inserted.");
        } finally {
          lockManager.releaseLock(Thread.currentThread(), i, OLockManager.LOCK.EXCLUSIVE);
        }
      }

      System.out.println(Thread.currentThread().getName() + ":Insertions were finished.");

      return null;
    }
  }

  private static final class ConcurrentReader implements Callable<Void> {
    private final OMerkleTreeNode node;
    private final CountDownLatch  trigger;
    private final AtomicBoolean   testIsFinished;

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
