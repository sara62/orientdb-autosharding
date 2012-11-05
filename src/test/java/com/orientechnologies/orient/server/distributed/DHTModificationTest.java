package com.orientechnologies.orient.server.distributed;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.orientechnologies.common.concur.lock.OLockManager;
import com.orientechnologies.common.util.MersenneTwister;
import com.orientechnologies.orient.server.hazelcast.ServerInstance;

/**
 * @author Andrey Lomakin
 * @since 25.10.12
 */
@Test
public class DHTModificationTest {
  public void testConstantAddRemoveWithoutDataManipulation() throws Exception {
    TreeMap<Long, ServerInstance> ringMap = new TreeMap<Long, ServerInstance>();

    final ServerInstance serverInstance = new ServerInstance();
    serverInstance.init();

    ringMap.put(serverInstance.getLocalNode().getNodeId(), serverInstance);

    for (int i = 0; i < 6; i++) {
      ServerInstance si = new ServerInstance();
      si.init();

      ringMap.put(si.getLocalNode().getNodeId(), si);
    }

    Thread.sleep(30000);
    checkDHTStructure(serverInstance, ringMap);

    final MersenneTwister random = new MersenneTwister();

    for (int i = 0; i < 60; i++) {
      Long nodeId = ringMap.ceilingKey(random.nextLong(Long.MAX_VALUE));
      if (nodeId == null)
        nodeId = ringMap.firstKey();

      ServerInstance si = ringMap.remove(nodeId);
      si.shutdown();

      Thread.sleep(30000);
      checkDHTStructure(ringMap.firstEntry().getValue(), ringMap);

      si = new ServerInstance();
      si.init();

      ringMap.put(si.getLocalNode().getNodeId(), si);

      Thread.sleep(30000);
      checkDHTStructure(si, ringMap);
    }
  }

  private void checkDHTStructure(ServerInstance serverInstance, TreeMap<Long, ServerInstance> ringMap) {
    ODHTNode node = serverInstance.findSuccessor(0);
    for (Long key : ringMap.keySet()) {
      Assert.assertEquals(node.getNodeId(), key.longValue());
      node = serverInstance.findById(node.getSuccessor());
    }

    Assert.assertEquals(node.getNodeId(), serverInstance.findSuccessor(0).getNodeId());
  }

  public void testConstantAddRemoveWithDataManipulation() throws Exception {
    final AtomicBoolean testIsStopped = new AtomicBoolean(false);
    final AtomicBoolean exceptionIsThrown = new AtomicBoolean(false);

    ExecutorService readerExecutor = Executors.newCachedThreadPool(new ThreadFactory() {
      public Thread newThread(Runnable r) {
        final Thread thread = new Thread(r);
        thread.setDaemon(true);
        return thread;
      }
    });

    ExecutorService writerExecutor = Executors.newCachedThreadPool(new ThreadFactory() {
      public Thread newThread(Runnable r) {
        final Thread thread = new Thread(r);
        thread.setDaemon(true);

        return thread;
      }
    });

    ExecutorService removalExecutor = Executors.newCachedThreadPool(new ThreadFactory() {
      public Thread newThread(Runnable r) {
        final Thread thread = new Thread(r);
        thread.setDaemon(true);

        return thread;
      }
    });

    final NavigableMap<Long, ServerInstance> ringMap = new ConcurrentSkipListMap<Long, ServerInstance>();

    final ServerInstance serverInstance = new ServerInstance(1, 1);
    serverInstance.init();

    ringMap.put(serverInstance.getLocalNode().getNodeId(), serverInstance);

    for (int i = 0; i < 3; i++) {
      ServerInstance si = new ServerInstance(1, 1);
      si.init();

      ringMap.put(si.getLocalNode().getNodeId(), si);
    }

    final NavigableMap<Long, Record> data = new ConcurrentSkipListMap<Long, Record>();
    final OLockManager<Long, Runnable> lockManager = new OLockManager<Long, Runnable>(true, 500);

    final List<Future<Void>> readerFutures = new ArrayList<Future<Void>>();

    List<Future<Void>> writerFutures = new ArrayList<Future<Void>>();

    final int threadCount = 2;
    final MersenneTwister mersenneTwister = new MersenneTwister();
    mersenneTwister.setSeed(new Random().nextLong());

    for (int i = 0; i < threadCount; i++)
      readerFutures.add(readerExecutor.submit(new DataReader(data, lockManager, ringMap, testIsStopped, exceptionIsThrown)));

    for (long i = 0; i < threadCount; i++)
      writerFutures.add(writerExecutor.submit(new DataWriter(mersenneTwister, data, lockManager, ringMap, testIsStopped,
          exceptionIsThrown)));

    Future<Void> removeFuture = removalExecutor
        .submit(new DataRemover(ringMap, data, lockManager, testIsStopped, exceptionIsThrown));

    final MersenneTwister random = new MersenneTwister();

    Thread.sleep(3 * 60 * 1000);
    if (exceptionIsThrown.get()) {
      testIsStopped.set(true);
      checkDataManipulationThreads(readerFutures, writerFutures, removeFuture);
    }

    for (int i = 0; i < 5; i++) {
      ServerInstance siToShutdown = extractOneNode(ringMap, random);
      System.out.println("Shutdown of node with id " + siToShutdown.getLocalNode().getNodeId());
      siToShutdown.shutdown();

      for (int n = 0; n < 3 + i; n++) {
        Thread.sleep(60 * 1000);

        if (exceptionIsThrown.get()) {
          testIsStopped.set(true);
          checkDataManipulationThreads(readerFutures, writerFutures, removeFuture);
        }
      }

      ServerInstance addedSI = new ServerInstance(1, 1);
      addedSI.init();

      ringMap.put(addedSI.getLocalNode().getNodeId(), addedSI);

      for (int n = 0; n < 3 + i; n++) {
        Thread.sleep(60 * 1000);

        if (exceptionIsThrown.get()) {
          testIsStopped.set(true);
          checkDataManipulationThreads(readerFutures, writerFutures, removeFuture);
        }
      }
    }

    testIsStopped.set(true);
    checkDataManipulationThreads(readerFutures, writerFutures, removeFuture);
  }

  private ServerInstance extractOneNode(NavigableMap<Long, ServerInstance> ringMap, MersenneTwister random) {
    Long nodeId = ringMap.ceilingKey(random.nextLong(Long.MAX_VALUE));
    if (nodeId == null)
      nodeId = ringMap.firstKey();

    return ringMap.remove(nodeId);
  }

  private void checkDataManipulationThreads(List<Future<Void>> readerFutures, List<Future<Void>> futures, Future<Void> removeFuture)
      throws InterruptedException, ExecutionException {
    System.out.println("[stat] Wait for writers .");
    for (Future<Void> future : futures)
      future.get();

    System.out.println("[stat] Wait for remover.");
    removeFuture.get();

    System.out.println("[stat] Wait for readers.");
    for (Future<Void> future : readerFutures)
      future.get();
  }

  private static class DataRemover implements Callable<Void> {
    private final OLockManager<Long, Runnable>       lockManager;
    private final Map<Long, Record>                  data;

    private final Random                             random = new Random();

    private final NavigableMap<Long, ServerInstance> ringMap;

    private final AtomicBoolean                      testIsStopped;
    private final AtomicBoolean                      exceptionIsThrown;

    private DataRemover(final NavigableMap<Long, ServerInstance> ringMap, Map<Long, Record> data,
        OLockManager<Long, Runnable> lockManager, AtomicBoolean testIsStopped, final AtomicBoolean exceptionIsThrown) {
      this.data = data;

      this.lockManager = lockManager;
      this.ringMap = ringMap;
      this.testIsStopped = testIsStopped;
      this.exceptionIsThrown = exceptionIsThrown;
    }

    public Void call() throws Exception {
      try {
        while (!testIsStopped.get()) {
          if (data.size() < 20000) {
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
              for (ServerInstance si : ringMap.values()) {
                lockManager.acquireLock(Thread.currentThread(), key, OLockManager.LOCK.EXCLUSIVE);
                try {
                  final Record record = data.get(key);

                  if (record != null) {
                    si.remove(key, record.getVersion());
                    data.remove(key);
                  }
                } catch (Exception e) {
                  if (!si.isRunning())
                    continue;

                  throw e;
                } finally {
                  lockManager.releaseLock(Thread.currentThread(), key, OLockManager.LOCK.EXCLUSIVE);
                }
                break;
              }
            }
            i++;
          }
        }
        return null;
      } catch (Exception e) {
        e.printStackTrace();
        exceptionIsThrown.set(true);
        throw e;
      }
    }
  }

  private static class DataWriter implements Callable<Void> {
    private final OLockManager<Long, Runnable>       lockManager;

    private final MersenneTwister                    random;
    private final Map<Long, Record>                  data;

    private final NavigableMap<Long, ServerInstance> ringMap;

    private final AtomicBoolean                      testIsStopped;
    private final AtomicBoolean                      exceptionIsThrown;

    private DataWriter(final MersenneTwister random, Map<Long, Record> data, OLockManager<Long, Runnable> lockManager,
        final NavigableMap<Long, ServerInstance> ringMap, AtomicBoolean testIsStopped, AtomicBoolean exceptionIsThrown) {
      this.random = random;
      this.data = data;
      this.lockManager = lockManager;

      this.ringMap = ringMap;
      this.testIsStopped = testIsStopped;
      this.exceptionIsThrown = exceptionIsThrown;
    }

    public Void call() throws Exception {
      long id = 0;
      try {
        while (!testIsStopped.get()) {
          if (data.size() > 50000) {
            Thread.sleep(100);
            continue;
          }

          kCycle: while (true) {
            id = random.nextLong(Long.MAX_VALUE);
            for (ServerInstance si : ringMap.values()) {
              lockManager.acquireLock(Thread.currentThread(), id, OLockManager.LOCK.EXCLUSIVE);
              try {
                if (data.containsKey(id))
                  continue kCycle;

                final Record record = si.create(id, String.valueOf(id));
                data.put(id, record);
              } catch (Exception e) {
                if (!si.isRunning())
                  continue;

                throw e;
              } finally {
                lockManager.releaseLock(Thread.currentThread(), id, OLockManager.LOCK.EXCLUSIVE);
              }

              break;
            }
            break;
          }
        }
        return null;
      } catch (Exception e) {
        e.printStackTrace();
        exceptionIsThrown.set(true);
        throw e;
      }
    }
  }

  private class DataReader implements Callable<Void> {
    private final Map<Long, Record>                  data;
    private final OLockManager<Long, Runnable>       lockManager;

    private final AtomicBoolean                      testIsStopped;
    private final AtomicBoolean                      exceptionIsThrown;
    private final NavigableMap<Long, ServerInstance> ringMap;

    public DataReader(Map<Long, Record> data, OLockManager<Long, Runnable> lockManager,
        final NavigableMap<Long, ServerInstance> ringMap, final AtomicBoolean testIsStopped, final AtomicBoolean exceptionIsThrown) {
      this.data = data;
      this.lockManager = lockManager;

      this.ringMap = ringMap;
      this.testIsStopped = testIsStopped;
      this.exceptionIsThrown = exceptionIsThrown;
    }

    public Void call() throws Exception {
      try {
        while (!testIsStopped.get()) {
          int i = 0;
          for (Map.Entry<Long, Record> entry : data.entrySet()) {
            if (testIsStopped.get())
              break;
            for (ServerInstance si : ringMap.values()) {
              lockManager.acquireLock(Thread.currentThread(), entry.getKey(), OLockManager.LOCK.SHARED);
              try {
                if (data.containsKey(entry.getKey()))
                  Assert.assertEquals(si.get(entry.getKey()), entry.getValue());

                i++;
                if (i % 10000 == 0)
                  System.out.println(Thread.currentThread().getName() + " " + i + " items were processed");

              } catch (Exception e) {
                if (!si.isRunning())
                  continue;

                throw e;
              } finally {
                lockManager.releaseLock(Thread.currentThread(), entry.getKey(), OLockManager.LOCK.SHARED);
              }
              break;
            }
          }
        }
        return null;
      } catch (Exception e) {
        e.printStackTrace();
        exceptionIsThrown.set(true);
        throw e;
      }
    }
  }

}
