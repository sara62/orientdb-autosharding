package com.orientechnologies.orient.server.distributed;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.orientechnologies.common.concur.lock.OLockManager;
import com.orientechnologies.common.util.MersenneTwisterFast;
import com.orientechnologies.orient.server.hazelcast.ServerInstance;

/**
 * @author Andrey Lomakin
 * @since 20.08.12
 */
@Test
public class DHTConcurrencyTest {
  private final AtomicBoolean testIsStopped   = new AtomicBoolean(false);

  private ExecutorService     readerExecutor  = Executors.newCachedThreadPool(new ThreadFactory() {
                                                public Thread newThread(Runnable r) {
                                                  final Thread thread = new Thread(r);
                                                  thread.setDaemon(true);
                                                  return thread;
                                                }
                                              });

  private ExecutorService     writerExecutor  = Executors.newCachedThreadPool(new ThreadFactory() {
                                                public Thread newThread(Runnable r) {
                                                  final Thread thread = new Thread(r);
                                                  thread.setDaemon(true);

                                                  return thread;
                                                }
                                              });

  private ExecutorService     removalExecutor = Executors.newCachedThreadPool(new ThreadFactory() {
                                                public Thread newThread(Runnable r) {
                                                  final Thread thread = new Thread(r);
                                                  thread.setDaemon(true);

                                                  return thread;
                                                }
                                              });

  public void addNode() throws Exception {
    ServerInstance serverInstance = new ServerInstance();
    serverInstance.init();
    while (true)
      ;
  }

  public void addRemoveData() throws Exception {
    final ServerInstance serverInstance = new ServerInstance();
    serverInstance.init();

    ServerInstance siNext = new ServerInstance();
    siNext.init();

    Thread.sleep(5000);

    final NavigableMap<Long, Record> data = new ConcurrentSkipListMap<Long, Record>();
    final OLockManager<Long, Runnable> lockManager = new OLockManager<Long, Runnable>(true, 500);

    final List<Future<Void>> readerFutures = new ArrayList<Future<Void>>();

    List<Future<Void>> futures = new ArrayList<Future<Void>>();

    final int threadCount = 5;

    for (int i = 0; i < threadCount; i++)
      readerFutures.add(readerExecutor.submit(new DataReader(data, lockManager, serverInstance)));

    for (long i = 0; i < threadCount; i++)
      futures.add(writerExecutor.submit(new DataWriter(data, lockManager, serverInstance, testIsStopped)));

    for (int i = 0; i < 5; i++) {
      ServerInstance si = new ServerInstance();
      si.init();

      Thread.sleep(20000);
    }

    Thread.sleep(20000);

    Future<Void> removeFuture = removalExecutor.submit(new DataRemover(serverInstance, data, lockManager, testIsStopped));

    testIsStopped.set(true);

    System.out.println("[stat] Wait for writers .");
    for (Future<Void> future : futures)
      future.get();

    System.out.println("[stat] Wait for remover.");
    removeFuture.get();

    System.out.println("[stat] Wait for readers.");
    for (Future<Void> future : readerFutures)
      future.get();

    ODHTNode startNode = serverInstance.findSuccessor(0);
    System.out.println("[stat] Wait till all nodes will be stable.");

    boolean allNodesAreStable = false;
    while (!allNodesAreStable) {
      ODHTNode node = serverInstance.findById(startNode.getSuccessor());
      allNodesAreStable = node.state().equals(ODHTNode.NodeState.PRODUCTION);
      while (node.getNodeId() != startNode.getNodeId() && allNodesAreStable) {
        allNodesAreStable = node.state().equals(ODHTNode.NodeState.PRODUCTION);

        node = serverInstance.findById(node.getSuccessor());
      }
    }

    System.out.println("[stat] Items check " + data.size() + " items.");
    int i = 0;

    long start = System.currentTimeMillis();
    for (Map.Entry<Long, Record> entry : data.entrySet()) {

      serverInstance.get(entry.getKey());
      Assert.assertEquals(serverInstance.get(entry.getKey()), entry.getValue(), "Key " + entry.getKey() + " is absent");
      i++;

      if (i % 10000 == 0) {
        System.out.println("[stat] " + i + " items were processed for " + (System.currentTimeMillis() - start) + " ms.");
        start = System.currentTimeMillis();
      }

    }

    System.out.println("[stat] wait to finish all asynchronous replications");

    Thread.sleep(60000);

    System.out.println("[stat] Node sizes : ");

    int totalSize = 0;

    System.out.println("[stat] Node : " + startNode.getNodeId() + " size - " + startNode.size() + " state " + startNode.state());
    totalSize += startNode.size();

    ODHTNode node = serverInstance.findById(startNode.getSuccessor());

    while (node.getNodeId() != startNode.getNodeId()) {
      System.out.println("[stat] Node : " + node.getNodeId() + " size - " + node.size() + " state " + node.state());

      totalSize += node.size();
      node = serverInstance.findById(node.getSuccessor());
    }

    Assert.assertEquals(totalSize, data.size() * (ServerInstance.REPLICA_COUNT + 1));
  }

  private static class DataRemover implements Callable<Void> {
    private final OLockManager<Long, Runnable> lockManager;
    private final Map<Long, Record>            data;

    private final Random                       random = new Random();

    private final ServerInstance               serverInstance;
    private final AtomicBoolean                testIsStopped;

    private DataRemover(ServerInstance serverInstance, Map<Long, Record> data, OLockManager<Long, Runnable> lockManager,
        AtomicBoolean testIsStopped) {
      this.data = data;

      this.lockManager = lockManager;
      this.serverInstance = serverInstance;
      this.testIsStopped = testIsStopped;
    }

    public Void call() throws Exception {
      try {
        while (!testIsStopped.get()) {
          if (data.size() < 100) {
            Thread.sleep(100);
            continue;
          }

          int n = random.nextInt(10);

          if (n < 5)
            n = 5;

          int i = 0;
          for (Long key : data.keySet()) {
            if (testIsStopped.get())
              break;

            if (i % n == 0) {
              lockManager.acquireLock(Thread.currentThread(), key, OLockManager.LOCK.EXCLUSIVE);
              try {
                final Record record = data.get(key);

                if (record != null) {
                  serverInstance.remove(key, record.getVersion());
                  data.remove(key);
                }
              } finally {
                lockManager.releaseLock(Thread.currentThread(), key, OLockManager.LOCK.EXCLUSIVE);
              }
            }
            i++;
          }
        }
        return null;
      } catch (Exception e) {
        System.out.println(e);
        throw e;
      }
    }
  }

  private static class DataWriter implements Callable<Void> {
    private final OLockManager<Long, Runnable> lockManager;

    private final MersenneTwisterFast          random = new MersenneTwisterFast();
    private final Map<Long, Record>            data;

    private final ServerInstance               serverInstance;
    private final AtomicBoolean                testIsStopped;

    private DataWriter(Map<Long, Record> data, OLockManager<Long, Runnable> lockManager, ServerInstance serverInstance,
        AtomicBoolean testIsStopped) {
      random.setSeed((new Random()).nextLong());
      this.data = data;
      this.lockManager = lockManager;

      this.serverInstance = serverInstance;
      this.testIsStopped = testIsStopped;
    }

    public Void call() throws Exception {
      long id = 0;
      try {
        while (!testIsStopped.get()) {
          id = random.nextLong(Long.MAX_VALUE);
          lockManager.acquireLock(Thread.currentThread(), id, OLockManager.LOCK.EXCLUSIVE);
          try {
            final Record record = serverInstance.create(id, String.valueOf(id));
            data.put(id, record);
          } finally {
            lockManager.releaseLock(Thread.currentThread(), id, OLockManager.LOCK.EXCLUSIVE);
          }
        }
        return null;
      } catch (Exception e) {
        System.out.println(e);
        throw e;
      }
    }
  }

  private class DataReader implements Callable<Void> {
    private final Map<Long, Record>            data;
    private final OLockManager<Long, Runnable> lockManager;

    private final ServerInstance               serverInstance;

    public DataReader(Map<Long, Record> data, OLockManager<Long, Runnable> lockManager, ServerInstance serverInstance) {
      this.data = data;
      this.lockManager = lockManager;

      this.serverInstance = serverInstance;
    }

    public Void call() throws Exception {
      try {
        while (!testIsStopped.get()) {
          int i = 0;
          for (Map.Entry<Long, Record> entry : data.entrySet()) {
            if (testIsStopped.get())
              break;

            lockManager.acquireLock(Thread.currentThread(), entry.getKey(), OLockManager.LOCK.SHARED);
            try {
              if (data.containsKey(entry.getKey()))
                Assert.assertEquals(serverInstance.get(entry.getKey()), entry.getValue(), "Key " + entry.getKey() + " is absent");
              i++;
              if (i % 10000 == 0)
                System.out.println(Thread.currentThread().getName() + " " + i + " items were processed");

            } finally {
              lockManager.releaseLock(Thread.currentThread(), entry.getKey(), OLockManager.LOCK.SHARED);
            }
          }
        }
        return null;
      } catch (Exception e) {
        System.out.println(e);
        throw e;
      }

    }
  }
}