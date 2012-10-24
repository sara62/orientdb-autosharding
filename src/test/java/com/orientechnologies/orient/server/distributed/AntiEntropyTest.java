package com.orientechnologies.orient.server.distributed;

import java.util.NavigableMap;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.orientechnologies.orient.server.hazelcast.OHazelcastDHTNodeProxy;
import com.orientechnologies.orient.server.hazelcast.ServerInstance;

/**
 * @author Andrey Lomakin
 * @since 22.10.12
 */
@Test
public class AntiEntropyTest {
  public void testDataSynchronizationAfterAdd() throws Exception {
    System.out.println("[stat] Ring initialization.");

    final ServerInstance serverInstance = new ServerInstance();
    serverInstance.init();

    for (int i = 0; i < 2; i++) {
      ServerInstance si = new ServerInstance(false, true, false);
      si.init();
    }

    Thread.sleep(10000);
    System.out.println("[stat] Ring was initialized.");
    System.out.println("[stat] Start data filling.");

    for (int i = 0; i < 1000; i++) {
      serverInstance.create("data" + i);
    }

    System.out.println("[stat] Data were added.");
    Thread.sleep(10000);

    checkDataRedistribution(serverInstance);

    ServerInstance si = new ServerInstance(false, true, false);
    si.init();

    Thread.sleep(10000);

    checkDataRedistribution(serverInstance);
  }

  private void checkDataRedistribution(ServerInstance serverInstance) {
    final ODHTNode startNode = serverInstance.findSuccessor(0);

    ODHTNode dhtNode = serverInstance.findSuccessor(0);

    do {
      System.out.println("[stat] Test replication of node " + dhtNode.getNodeId() + ".");

      final OLocalDHTNode localDHTNode = getLocalNode(dhtNode);

      final ODHTNode firstSuccessor = serverInstance.findById(dhtNode.getSuccessor());
      final OLocalDHTNode localFirstSuccessor = getLocalNode(firstSuccessor);

      final ODHTNode secondSuccessor = serverInstance.findById(firstSuccessor.getSuccessor());
      final OLocalDHTNode localSecondSuccessor = getLocalNode(secondSuccessor);

      final NavigableMap<Long, Record> nodeDb = localDHTNode.getDb();
      final NavigableMap<Long, Record> firstSuccessorDb = localFirstSuccessor.getDb();
      final NavigableMap<Long, Record> secondSuccessorDb = localSecondSuccessor.getDb();

      ODHTRingIterator ringIterator = new ODHTRingIterator(nodeDb, ODHTRingInterval.nextValue(dhtNode.getPredecessor()),
          dhtNode.getNodeId());

      while (ringIterator.hasNext()) {
        final RecordMetadata recordMetadata = ringIterator.next();

        Assert.assertTrue(firstSuccessorDb.containsKey(recordMetadata.getId()));
        Assert.assertTrue(secondSuccessorDb.containsKey(recordMetadata.getId()));
      }

      dhtNode = serverInstance.findById(dhtNode.getSuccessor());
    } while (dhtNode.getNodeId() != startNode.getNodeId());
  }

  private OLocalDHTNode getLocalNode(ODHTNode dhtNode) {
    final OLocalDHTNode localDHTNode;
    if (dhtNode instanceof OLocalDHTNode)
      localDHTNode = (OLocalDHTNode) dhtNode;
    else
      localDHTNode = ServerInstance.INSTANCES.get(((OHazelcastDHTNodeProxy) dhtNode).getMemberUUID()).getLocalNode();

    return localDHTNode;
  }
}
