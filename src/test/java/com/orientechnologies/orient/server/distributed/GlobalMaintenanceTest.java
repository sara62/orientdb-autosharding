package com.orientechnologies.orient.server.distributed;

import java.util.NavigableMap;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.id.OAutoShardedRecordId;
import com.orientechnologies.orient.server.hazelcast.OHazelcastDHTNodeProxy;
import com.orientechnologies.orient.server.hazelcast.ServerInstance;

/**
 * @author Andrey Lomakin
 * @since 25.10.12
 */
@Test
public class GlobalMaintenanceTest {

  public void testNodeReplicaRedistribution() throws Exception {
    System.out.println("[stat] Ring initialization.");

    final ServerInstance serverInstance = new ServerInstance();
    serverInstance.init();

    for (int i = 0; i < 2; i++) {
      ServerInstance si = new ServerInstance(false, false, true);
      si.init();
    }

    Thread.sleep(10000);
    System.out.println("[stat] Ring was initialized.");
    System.out.println("[stat] Start data filling.");

    for (int i = 0; i < 100000; i++) {
      serverInstance.create("data" + i);
    }

    System.out.println("[stat] Data were added.");
    Thread.sleep(5000);

    checkDataRedistribution(serverInstance);

    ServerInstance si = new ServerInstance(false, true, false);
    si.init();

    System.out.println("[stat] Global maintenance protocol check");
    Thread.sleep(240000);

    checkDataRedistributionWhenNewNodeWasAdded(serverInstance);

    final OLocalDHTNode localDHTNode = si.getLocalNode();
    final OLocalDHTNode localPredecessorOne = getLocalNode(si.findById(localDHTNode.getPredecessor()));
    final OLocalDHTNode localPredecessorTwo = getLocalNode(si.findById(localPredecessorOne.getPredecessor()));

    int dhtNodeRecords = getOwnRecordsCount(localDHTNode);
    int predecessorOneRecords = getOwnRecordsCount(localPredecessorOne);
    int predecessorTwoRecords = getOwnRecordsCount(localPredecessorTwo);

    Assert.assertEquals(localDHTNode.getDb().size(), dhtNodeRecords + predecessorOneRecords + predecessorTwoRecords);
  }

  private void checkDataRedistribution(ServerInstance serverInstance) throws Exception {
    final ODHTNode startNode = serverInstance.findSuccessor(ONodeId.valueOf(0));

    ODHTNode dhtNode = serverInstance.findSuccessor(ONodeId.valueOf(0));

    do {
      System.out.println("[stat] Test replication of node " + dhtNode.getNodeAddress() + ".");

      final OLocalDHTNode localDHTNode = getLocalNode(dhtNode);

      final ODHTNode firstSuccessor = serverInstance.findById(dhtNode.getSuccessor());
      final OLocalDHTNode localFirstSuccessor = getLocalNode(firstSuccessor);

      final ODHTNode secondSuccessor = serverInstance.findById(firstSuccessor.getSuccessor());
      final OLocalDHTNode localSecondSuccessor = getLocalNode(secondSuccessor);

      final NavigableMap<OAutoShardedRecordId, Record> nodeDb = localDHTNode.getDb();
      final NavigableMap<OAutoShardedRecordId, Record> firstSuccessorDb = localFirstSuccessor.getDb();
      final NavigableMap<OAutoShardedRecordId, Record> secondSuccessorDb = localSecondSuccessor.getDb();

      ODHTRingIterator ringIterator = new ODHTRingIterator(nodeDb, ONodeId.convertToRecordId(dhtNode.getPredecessor().getNodeId()
          .add(ONodeId.ONE), 1), ONodeId.convertToRecordId(dhtNode.getNodeAddress().getNodeId(), 1));

      while (ringIterator.hasNext()) {
        final RecordMetadata recordMetadata = ringIterator.next();

        Assert.assertTrue(firstSuccessorDb.containsKey(recordMetadata.getId()));
        Assert.assertTrue(secondSuccessorDb.containsKey(recordMetadata.getId()));
      }

      dhtNode = serverInstance.findById(dhtNode.getSuccessor());
    } while (!dhtNode.getNodeAddress().equals(startNode.getNodeAddress()));
  }

  private void checkDataRedistributionWhenNewNodeWasAdded(ServerInstance serverInstance) throws Exception {
    final ODHTNode startNode = serverInstance.findSuccessor(ONodeId.valueOf(0));

    ODHTNode dhtNode = serverInstance.findSuccessor(ONodeId.valueOf(0));

    do {
      System.out.println("[stat] Test replication of node " + dhtNode.getNodeAddress() + ".");

      final OLocalDHTNode localDHTNode = getLocalNode(dhtNode);

      final ODHTNode firstSuccessor = serverInstance.findById(dhtNode.getSuccessor());
      final OLocalDHTNode localFirstSuccessor = getLocalNode(firstSuccessor);

      final ODHTNode secondSuccessor = serverInstance.findById(firstSuccessor.getSuccessor());
      final OLocalDHTNode localSecondSuccessor = getLocalNode(secondSuccessor);

      final ODHTNode thirdSuccessor = serverInstance.findById(secondSuccessor.getSuccessor());
      final OLocalDHTNode localThirdSuccessor = getLocalNode(thirdSuccessor);

      final NavigableMap<OAutoShardedRecordId, Record> nodeDb = localDHTNode.getDb();
      final NavigableMap<OAutoShardedRecordId, Record> firstSuccessorDb = localFirstSuccessor.getDb();
      final NavigableMap<OAutoShardedRecordId, Record> secondSuccessorDb = localSecondSuccessor.getDb();
      final NavigableMap<OAutoShardedRecordId, Record> thirdSuccessorDb = localThirdSuccessor.getDb();

      ODHTRingIterator ringIterator = new ODHTRingIterator(nodeDb, ONodeId.convertToRecordId(dhtNode.getPredecessor().getNodeId()
          .add(ONodeId.ONE), 1), ONodeId.convertToRecordId(dhtNode.getNodeAddress().getNodeId(), 1));

      while (ringIterator.hasNext()) {
        final RecordMetadata recordMetadata = ringIterator.next();

        Assert.assertTrue(firstSuccessorDb.containsKey(recordMetadata.getId()));
        Assert.assertTrue(secondSuccessorDb.containsKey(recordMetadata.getId()));

        Assert.assertFalse(thirdSuccessorDb.containsKey(recordMetadata.getId()));
      }

      dhtNode = serverInstance.findById(dhtNode.getSuccessor());
    } while (!dhtNode.getNodeAddress().equals(startNode.getNodeAddress()));
  }

  private OLocalDHTNode getLocalNode(ODHTNode dhtNode) {
    final OLocalDHTNode localDHTNode;
    if (dhtNode instanceof OLocalDHTNode)
      localDHTNode = (OLocalDHTNode) dhtNode;
    else
      localDHTNode = ServerInstance.INSTANCES.get(((OHazelcastDHTNodeProxy) dhtNode).getMemberUUID()).getLocalNode();

    return localDHTNode;
  }

  private int getOwnRecordsCount(OLocalDHTNode localDHTNode) {
    final NavigableMap<OAutoShardedRecordId, Record> nodeDb = localDHTNode.getDb();

    ODHTRingIterator ringIterator = new ODHTRingIterator(nodeDb, ONodeId.convertToRecordId(localDHTNode.getPredecessor()
        .getNodeId().add(ONodeId.ONE), 1), ONodeId.convertToRecordId(localDHTNode.getNodeAddress().getNodeId(), 1));

    int count = 0;
    while (ringIterator.hasNext()) {
      final RecordMetadata recordMetadata = ringIterator.next();
      count++;
    }

    return count;
  }
}
