package com.orientechnologies.orient.server.distributed.integration;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.hazelcast.util.ConcurrentHashSet;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.id.OClusterPositionNodeId;
import com.orientechnologies.orient.core.id.ONodeId;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.distributed.ODHTDatabaseLookup;
import com.orientechnologies.orient.server.distributed.ODHTDatabaseLookupImpl;
import com.orientechnologies.orient.server.distributed.merkletree.OMerkleTreeNode;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.orientechnologies.common.concur.lock.OLockManager;

/**
 * @author Andrey Lomakin
 * @since 13.09.12
 */
@Test
public class MerkleTreeNodeTestMultithreadingTest {
  public static final int     CLUSTER_ID = 1;
  private ODatabaseDocumentTx db;
  private ODatabaseDocumentTx db2;

  @BeforeMethod
  public void setUp() {
    OGlobalConfiguration.DB_USE_DISTRIBUTED_VERSION.setValue(true);
    OGlobalConfiguration.USE_NODE_ID_CLUSTER_POSITION.setValue(true);
    OGlobalConfiguration.USE_LHPEPS_MEMORY_CLUSTER.setValue(true);
    OGlobalConfiguration.STORAGE_USE_TOMBSTONES.setValue(true);

    db = new ODatabaseDocumentTx("memory:mercleTreeMultithreadingTest");
    db.create();

    db2 = new ODatabaseDocumentTx("memory:mercleTreeMultithreadingTestSecondaryDb");
    db2.create();
  }

  @AfterMethod
  public void tearDown() throws Exception {
    db.drop();
    db2.drop();
  }

  public void testConcurrentModifications() throws Exception {
    final ExecutorService adderExecutorService = Executors.newCachedThreadPool(new AdderThreadFactory());
    final ExecutorService readerExecutorService = Executors.newCachedThreadPool(new ReaderThreadFactory());
    final ExecutorService deleterExecutorService = Executors.newCachedThreadPool(new DeleterThreadFactory());

    final CountDownLatch trigger = new CountDownLatch(1);

    final OMerkleTreeNode treeNode = new OMerkleTreeNode(new ODHTDatabaseLookupImpl("memory:mercleTreeMultithreadingTest", "admin","admin"), 1);

    final List<Future<Void>> writerFutures = new ArrayList<Future<Void>>();
    final List<Future<Void>> readerFutures = new ArrayList<Future<Void>>();
    final List<Future<Void>> deleterFutures = new ArrayList<Future<Void>>();

    final AtomicBoolean testIsFinished = new AtomicBoolean(false);

    final OLockManager<ORID, Runnable> lockManager = new OLockManager<ORID, Runnable>(true, 500);

    final ConcurrentHashSet<ONodeId> ids = new ConcurrentHashSet<ONodeId>();

    final long start = System.currentTimeMillis();

    final int interval = 500000;

    for (int i = 0; i < 1; i++) //5
      writerFutures.add(adderExecutorService.submit(new ConcurrentAdder(treeNode, trigger, i * interval, interval, lockManager, ids)));

    for (int i = 0; i < 0; i++) //2
      readerFutures.add(readerExecutorService.submit(new ConcurrentReader(treeNode, trigger, testIsFinished)));

//    deleterFutures.add(deleterExecutorService.submit(new ConcurrentDeleter(treeNode, trigger, interval * 5, testIsFinished,
//        lockManager, new ODHTDatabaseLookupImpl("memory:mercleTreeMultithreadingTest", "admin","admin"), ids)));

    trigger.countDown();

    for (Future<Void> future : writerFutures)
      future.get();

    testIsFinished.set(true);

    for (Future<Void> future : readerFutures)
      future.get();

    for (Future<Void> future : deleterFutures)
      future.get();

    System.out.println("Time spent : " + (System.currentTimeMillis() - start));

    final OMerkleTreeNode sampleTreeNode = new OMerkleTreeNode(new ODHTDatabaseLookupImpl("memory:mercleTreeMultithreadingTestSecondaryDb", "admin","admin"), 1);

    System.out.println("Sample tree creation. DB size is : " + db.getSize());

    for (ODocument doc : db.browseCluster(db.getClusterNameById(CLUSTER_ID), new OClusterPositionNodeId(ONodeId.MIN_VALUE),
        new OClusterPositionNodeId(ONodeId.MAX_VALUE), true)) {
      int childPos = OMerkleTreeNode.childIndex(0, ((OClusterPositionNodeId) doc.getIdentity().getClusterPosition()).getNodeId());
      ONodeId startKey = OMerkleTreeNode.startNodeId(1, childPos, ONodeId.ZERO);

      sampleTreeNode.updateReplica(1, startKey, doc.getIdentity(), doc);
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

  private static final class ConcurrentDeleter implements Callable<Void> {
    private final OMerkleTreeNode              node;
    private final CountDownLatch               trigger;
    private final int                          interval;

    private final AtomicBoolean                testIsFinished;

    private final Random                       random = new Random();

    private final ODHTDatabaseLookup dbLookup;

    private final OLockManager<ORID, Runnable> lockManager;
    private final ConcurrentHashSet<ONodeId> ids;

    private ConcurrentDeleter(OMerkleTreeNode node, CountDownLatch trigger, int interval, AtomicBoolean testIsFinished,
                              OLockManager<ORID, Runnable> lockManager, ODHTDatabaseLookupImpl dbLookup, ConcurrentHashSet<ONodeId> ids) {
      this.node = node;
      this.trigger = trigger;
      this.interval = interval;
      this.testIsFinished = testIsFinished;
      this.lockManager = lockManager;
      this.dbLookup = dbLookup;
      this.ids = ids;
    }

    public Void call() throws Exception {
      trigger.await();

      while (!testIsFinished.get()) {
        final ONodeId key = ids.iterator().next();
				final ORID rid = new ORecordId(1, new OClusterPositionNodeId(key));

        lockManager.acquireLock(Thread.currentThread(), rid, OLockManager.LOCK.EXCLUSIVE);
        try {
          final ODatabaseRecord db = dbLookup.openDatabase("memory:mercleTreeMultithreadingTest");
          final ORecordInternal<?> record = db.load(rid);

          if (record == null || record.getRecordVersion().isTombstone())
            continue;

          final int childPos = OMerkleTreeNode.childIndex(0, key);
          final ONodeId startKey = OMerkleTreeNode.startNodeId(1, childPos, ONodeId.ZERO);

          node.deleteRecord(1, startKey, rid, record.getRecordVersion());
          ids.remove(key);
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
    private final ConcurrentHashSet<ONodeId> ids;

    private ConcurrentAdder(OMerkleTreeNode node, CountDownLatch trigger, long startPos, int interval,
                            OLockManager<ORID, Runnable> lockManager, ConcurrentHashSet<ONodeId> ids) {
      this.node = node;
      this.trigger = trigger;
      this.startPos = startPos;
      this.interval = interval;
      this.lockManager = lockManager;
      this.ids = ids;
    }

    public Void call() throws Exception {
      trigger.await();

      System.out.println(Thread.currentThread().getName() + ":Insertions were started.");

      for (long i = startPos; i < startPos + interval; i++) {
				final ONodeId nodeId = ONodeId.generateUniqueId().shiftRight(6);
				final ORecordId rid = new ORecordId(1, new OClusterPositionNodeId(nodeId));

        int childPos = OMerkleTreeNode.childIndex(0, nodeId);
        ONodeId startKey = OMerkleTreeNode.startNodeId(1, childPos, ONodeId.ZERO);

        lockManager.acquireLock(Thread.currentThread(), rid, OLockManager.LOCK.EXCLUSIVE);
        try {
          final ODocument doc = new ODocument();
          doc.setIdentity(rid);
          doc.field("value", i + "");
          node.addRecord(1, startKey, doc);
          ids.add(nodeId);
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
