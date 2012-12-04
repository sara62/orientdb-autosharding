package com.orientechnologies.orient.server.distributed.integration;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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

import com.orientechnologies.orient.core.id.OClusterPositionNodeId;
import com.orientechnologies.orient.core.id.ONodeId;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.server.distributed.Record;
import com.orientechnologies.orient.server.distributed.merkletree.OMerkleTreeNode;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.orientechnologies.common.concur.lock.OLockManager;

/**
 * @author Andrey Lomakin
 * @since 13.09.12
 */
@Test
public class MerkleTreeNodeTestMultithreadingTest {
  public void testConcurrentModifications() throws Exception {
    final ExecutorService adderExecutorService = Executors.newCachedThreadPool(new AdderThreadFactory());
    final ExecutorService readerExecutorService = Executors.newCachedThreadPool(new ReaderThreadFactory());
    final ExecutorService deleterExecutorService = Executors.newCachedThreadPool(new DeleterThreadFactory());

    final NavigableMap<ORID, Record> db = new ConcurrentSkipListMap<ORID, Record>();
    final CountDownLatch trigger = new CountDownLatch(1);

    final OMerkleTreeNode treeNode = new OMerkleTreeNode(db, 1);

    final List<Future<Void>> writerFutures = new ArrayList<Future<Void>>();
    final List<Future<Void>> readerFutures = new ArrayList<Future<Void>>();
    final List<Future<Void>> deleterFutures = new ArrayList<Future<Void>>();

    final AtomicBoolean testIsFinished = new AtomicBoolean(false);

    final OLockManager<ORID, Runnable> lockManager = new OLockManager<ORID, Runnable>(true, 500);

    final long start = System.currentTimeMillis();

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

    System.out.println("Time spent : " + (System.currentTimeMillis() - start));

    final NavigableMap<ORID, Record> dbTwo = new ConcurrentSkipListMap<ORID, Record>();
    final OMerkleTreeNode sampleTreeNode = new OMerkleTreeNode(dbTwo, 1);

    System.out.println("Sample tree creation. DB size is : " + db.size());

    for (Map.Entry<ORID, Record> entry : db.entrySet()) {
      int childPos = OMerkleTreeNode.childIndex(0, ((OClusterPositionNodeId)entry.getKey().getClusterPosition()).getNodeId());
      ONodeId startKey = OMerkleTreeNode.startNodeId(1, childPos, ONodeId.ZERO);

      sampleTreeNode.updateReplica(1, startKey, entry.getKey(), entry.getValue());
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

    private final NavigableMap<ORID, Record>   db;

    private final OLockManager<ORID, Runnable> lockManager;

    private ConcurrentDeleter(OMerkleTreeNode node, CountDownLatch trigger, int interval, AtomicBoolean testIsFinished,
        OLockManager<ORID, Runnable> lockManager, NavigableMap<ORID, Record> db) {
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
        final ONodeId key = ONodeId.valueOf(random.nextInt(interval));
				final ORID rid = new ORecordId(1, new OClusterPositionNodeId(key));

        lockManager.acquireLock(Thread.currentThread(), rid, OLockManager.LOCK.EXCLUSIVE);
        try {
          final Record record = db.get(rid);

          if (record == null || record.isTombstone())
            continue;

          final int childPos = OMerkleTreeNode.childIndex(0, key);
          final ONodeId startKey = OMerkleTreeNode.startNodeId(1, childPos, ONodeId.ZERO);

          node.deleteRecord(1, startKey, rid, record.getVersion());
        } finally {
          lockManager.releaseLock(Thread.currentThread(), rid, OLockManager.LOCK.EXCLUSIVE);
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
    private final OLockManager<ORID, Runnable> lockManager;

    private ConcurrentAdder(OMerkleTreeNode node, CountDownLatch trigger, long startPos, int interval,
        OLockManager<ORID, Runnable> lockManager) {
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
				final ONodeId nodeId = ONodeId.valueOf(i);
				final ORID rid = new ORecordId(1, new OClusterPositionNodeId(nodeId));

        int childPos = OMerkleTreeNode.childIndex(0, nodeId);
        ONodeId startKey = OMerkleTreeNode.startNodeId(1, childPos, ONodeId.ZERO);

        lockManager.acquireLock(Thread.currentThread(), rid, OLockManager.LOCK.EXCLUSIVE);
        try {
          node.addRecord(1, startKey, rid, i + "");
          if ((i - startPos) % 10000 == 0)
            System.out.println(Thread.currentThread().getName() + ":" + (i - startPos) + " records were inserted.");
        } finally {
          lockManager.releaseLock(Thread.currentThread(), rid, OLockManager.LOCK.EXCLUSIVE);
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
