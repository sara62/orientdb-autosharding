package com.orientechnologies.orient.server.distributed.integration;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.orientechnologies.common.concur.lock.OLockManager;
import com.orientechnologies.common.util.MersenneTwisterFast;
import com.orientechnologies.orient.core.id.OClusterPositionNodeId;
import com.orientechnologies.orient.core.id.ONodeId;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.server.distributed.ODHTNode;
import com.orientechnologies.orient.server.distributed.Record;
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
		OGlobalConfiguration.DB_USE_DISTRIBUTED_VERSION.setValue(true);

    final ServerInstance serverInstance = new ServerInstance();
    serverInstance.init();

    ServerInstance siNext = new ServerInstance();
    siNext.init();

    Thread.sleep(5000);

    final NavigableMap<ORID, Record> data = new ConcurrentSkipListMap<ORID, Record>();
    final OLockManager<ORID, Runnable> lockManager = new OLockManager<ORID, Runnable>(true, 500);

    final List<Future<Void>> readerFutures = new ArrayList<Future<Void>>();

    List<Future<Void>> futures = new ArrayList<Future<Void>>();

    final int threadCount = 5;

    for (int i = 0; i < threadCount; i++)
      readerFutures.add(readerExecutor.submit(new DataReader(data, lockManager, serverInstance)));

    for (long i = 0; i < threadCount; i++)
      futures.add(writerExecutor.submit(new DataWriter(data, lockManager, serverInstance, testIsStopped)));

    Future<Void> removeFuture = removalExecutor.submit(new DataRemover(serverInstance, data, lockManager, testIsStopped));

    for (int i = 0; i < 5; i++) {
      ServerInstance si = new ServerInstance();
      si.init();

      Thread.sleep(60000);
    }

    Thread.sleep(60000);

    testIsStopped.set(true);

    System.out.println("[stat] Wait for writers .");
    for (Future<Void> future : futures)
      future.get();

    System.out.println("[stat] Wait for remover.");
    removeFuture.get();

    System.out.println("[stat] Wait for readers.");
    for (Future<Void> future : readerFutures)
      future.get();

    ODHTNode startNode = serverInstance.findSuccessor(ONodeId.valueOf(0));
    System.out.println("[stat] Wait till all nodes will be stable.");

    boolean allNodesAreStable = false;
    while (!allNodesAreStable) {
      ODHTNode node = serverInstance.findById(startNode.getSuccessor());
      allNodesAreStable = node.state().equals(ODHTNode.NodeState.PRODUCTION);
      while (!node.getNodeAddress().equals(startNode.getNodeAddress()) && allNodesAreStable) {
        allNodesAreStable = node.state().equals(ODHTNode.NodeState.PRODUCTION);

        node = serverInstance.findById(node.getSuccessor());
      }
    }

    System.out.println("[stat] Items check " + data.size() + " items.");
    int i = 0;

    long start = System.currentTimeMillis();
    for (Map.Entry<ORID, Record> entry : data.entrySet()) {

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

    System.out.println("[stat] Node : " + startNode.getNodeAddress() + " size - " + startNode.size(null, -1) + " state "
        + startNode.state());
    totalSize += startNode.size(null, -1);

    ODHTNode node = serverInstance.findById(startNode.getSuccessor());

    while (!node.getNodeAddress().equals(startNode.getNodeAddress())) {
      System.out.println("[stat] Node : " + node.getNodeAddress() + " size - " + node.size(null, -1) + " state " + node.state());

      totalSize += node.size(null, -1);
      node = serverInstance.findById(node.getSuccessor());
    }

    Assert.assertEquals(totalSize, data.size() * (ServerInstance.REPLICA_COUNT + 1));
  }

  private static class DataRemover implements Callable<Void> {
    private final OLockManager<ORID, Runnable> lockManager;
    private final Map<ORID, Record>            data;

    private final Random                       random = new Random();

    private final ServerInstance               serverInstance;
    private final AtomicBoolean                testIsStopped;

    private DataRemover(ServerInstance serverInstance, Map<ORID, Record> data, OLockManager<ORID, Runnable> lockManager,
        AtomicBoolean testIsStopped) {
      this.data = data;

      this.lockManager = lockManager;
      this.serverInstance = serverInstance;
      this.testIsStopped = testIsStopped;
    }

    public Void call() throws Exception {
      Thread.currentThread().setName("DataRemover thread");

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
          for (ORID key : data.keySet()) {
            if (testIsStopped.get())
              break;

            if (i % n == 0) {
              lockManager.acquireLock(Thread.currentThread(), key, OLockManager.LOCK.EXCLUSIVE);
              try {
                Record record = data.get(key);

                if (record != null) {
                  record = serverInstance.get(key);

                  Assert.assertNotNull(record);

                  while (true)
                    try {
                      serverInstance.remove(key, record.getVersion());
                      break;
                    } catch (Exception e) {
											if (e.getCause() instanceof ExecutionException && e.getCause().getCause() instanceof ORecordNotFoundException) {
												System.out.println("[stat] Reread data in deleter for record with id " + key);

												record = serverInstance.get(key);

												Assert.assertNotNull(record);
											} else
												throw e;
                    }

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
        e.printStackTrace();
        throw e;
      }
    }
  }

  private static class DataWriter implements Callable<Void> {
    private final OLockManager<ORID, Runnable> lockManager;

    private final MersenneTwisterFast          random = new MersenneTwisterFast();
    private final Map<ORID, Record>            data;

    private final ServerInstance               serverInstance;
    private final AtomicBoolean                testIsStopped;

    private DataWriter(Map<ORID, Record> data, OLockManager<ORID, Runnable> lockManager, ServerInstance serverInstance,
        AtomicBoolean testIsStopped) {
      random.setSeed((new Random()).nextLong());
      this.data = data;
      this.lockManager = lockManager;

      this.serverInstance = serverInstance;
      this.testIsStopped = testIsStopped;
    }

    public Void call() throws Exception {
      ORID id;
      try {
        while (!testIsStopped.get()) {
          id = new ORecordId(1, new OClusterPositionNodeId(ONodeId.generateUniqueId()));
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
        e.printStackTrace();
        throw e;
      }
    }
  }

  private class DataReader implements Callable<Void> {
    private final Map<ORID, Record>            data;
    private final OLockManager<ORID, Runnable> lockManager;

    private final ServerInstance               serverInstance;

    public DataReader(Map<ORID, Record> data, OLockManager<ORID, Runnable> lockManager, ServerInstance serverInstance) {
      this.data = data;
      this.lockManager = lockManager;

      this.serverInstance = serverInstance;
    }

    public Void call() throws Exception {
      try {
        while (!testIsStopped.get()) {
          int i = 0;
          for (Map.Entry<ORID, Record> entry : data.entrySet()) {
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
        e.printStackTrace();
        throw e;
      }

    }
  }
}
