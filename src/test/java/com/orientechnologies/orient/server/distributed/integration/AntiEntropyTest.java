package com.orientechnologies.orient.server.distributed.integration;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.distributed.ODHTDatabaseLookup;
import com.orientechnologies.orient.server.distributed.ODHTDatabaseLookupImpl;
import com.orientechnologies.orient.server.distributed.ODHTNode;
import com.orientechnologies.orient.server.distributed.ODatabaseRingIterator;
import com.orientechnologies.orient.server.distributed.OLocalDHTNode;
import com.orientechnologies.orient.server.distributed.ORecordMetadata;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.id.OClusterPositionNodeId;
import com.orientechnologies.orient.core.id.ONodeId;
import com.orientechnologies.orient.server.hazelcast.OHazelcastDHTNodeProxy;
import com.orientechnologies.orient.server.hazelcast.ServerInstance;

/**
 * @author Andrey Lomakin
 * @since 22.10.12
 */
@Test
public class AntiEntropyTest {
  public static final int DB_COUNT = 10;
  private ODatabaseDocumentTx[] db;

  @BeforeMethod
  public void setUp() {
    OGlobalConfiguration.DB_USE_DISTRIBUTED_VERSION.setValue(true);
    OGlobalConfiguration.USE_NODE_ID_CLUSTER_POSITION.setValue(true);
    OGlobalConfiguration.USE_LHPEPS_MEMORY_CLUSTER.setValue(true);
    OGlobalConfiguration.STORAGE_USE_TOMBSTONES.setValue(true);

    db = new ODatabaseDocumentTx[DB_COUNT];
    for (int i = 0; i < DB_COUNT; i++) {
      db[i] = new ODatabaseDocumentTx("memory:AntiEntropyTest" + i);
      db[i].create();
    }
  }

  @AfterMethod
  public void tearDown() throws Exception {
    for (int i = 0; i < 5; i++) {
      db[i].drop();
    }
  }
  public void testDataSynchronizationAfterAdd() throws Exception {
    System.out.println("[stat] Ring initialization.");

    final ServerInstance serverInstance = new ServerInstance(dbLookup(0), false, true, false);
    serverInstance.init();

    for (int i = 1; i < 5; i++) {
      ServerInstance si = new ServerInstance(dbLookup(i), false, true, false);
      si.init();
    }

    Thread.sleep(10000);
    System.out.println("[stat] Ring was initialized.");
    System.out.println("[stat] Start data filling.");

    for (int i = 0; i < 100000; i++) {

      final ODocument doc = new ODocument();
      doc.field("value", i + "");
      
      serverInstance.create(doc, "storageName");
    }

    System.out.println("[stat] Data were added.");
    Thread.sleep(5000);

    checkDataRedistribution(serverInstance);

    ServerInstance si = new ServerInstance(dbLookup(5), false, true, false);
    si.init();

    System.out.println("[stat] Local maintenance protocol check");
    Thread.sleep(240000);

    checkDataRedistribution(serverInstance);

    final OLocalDHTNode localDHTNode = si.getLocalNode();
    final OLocalDHTNode localPredecessorOne = getLocalNode(si.findById(localDHTNode.getPredecessor()));
    final OLocalDHTNode localPredecessorTwo = getLocalNode(si.findById(localPredecessorOne.getPredecessor()));

    int dhtNodeRecords = gerOwnRecordsCount(localDHTNode);
    int predecessorOneRecords = gerOwnRecordsCount(localPredecessorOne);
    int predecessorTwoRecords = gerOwnRecordsCount(localPredecessorTwo);

    Assert.assertEquals(localDHTNode.getDb(null).getSize(), dhtNodeRecords + predecessorOneRecords + predecessorTwoRecords);
  }

  public void testDataSynchronizationAfterRemove() throws Exception {
    System.out.println("[stat] Ring initialization.");

    List<ServerInstance> serverInstances = new ArrayList<ServerInstance>();

    final ServerInstance serverInstance = new ServerInstance(dbLookup(0), false, true, false);
    serverInstance.init();

    serverInstances.add(serverInstance);

    for (int i = 1; i < 10; i++) {
      ServerInstance si = new ServerInstance(dbLookup(i), false, true, false);
      si.init();

      serverInstances.add(si);
    }

    Thread.sleep(10000);
    System.out.println("[stat] Ring was initialized.");
    System.out.println("[stat] Start data filling.");

    for (int i = 0; i < 100000; i++) {

      final ODocument doc = new ODocument();
      doc.field("value", i + "");

      serverInstance.create("storageName", doc);
    }

    System.out.println("[stat] Data were added.");
    Thread.sleep(5000);

    checkDataRedistribution(serverInstance);

    final Random random = new Random();
    while (serverInstances.size() > 1) {
      ServerInstance si = serverInstances.remove(random.nextInt(serverInstances.size()));
      si.shutdown();

      System.out.println("Node " + si.getLocalNode().getNodeAddress() + " was removed.");

      System.out.println("[stat] Local maintenance protocol check");
      Thread.sleep(30000);

      checkDataRedistribution(serverInstances.get(0));
    }
  }

  public void testDataSynchronizationAfterRemoveAddRemove() throws Exception {
    System.out.println("[stat] Ring initialization.");

    List<ServerInstance> serverInstances = new ArrayList<ServerInstance>();

    final ServerInstance serverInstance = new ServerInstance(dbLookup(0), false, true, false);
    serverInstance.init();

    serverInstances.add(serverInstance);

    for (int i = 1; i < 5; i++) {
      ServerInstance si = new ServerInstance(dbLookup(i), false, true, false);
      si.init();

      serverInstances.add(si);
    }

    Thread.sleep(10000);
    System.out.println("[stat] Ring was initialized.");
    System.out.println("[stat] Start data filling.");

    for (int i = 0; i < 100000; i++) {

      final ODocument doc = new ODocument();
      doc.field("value", i + "");

      serverInstance.create("storage", doc);
    }

    System.out.println("[stat] Data were added.");
    Thread.sleep(5000);

    checkDataRedistribution(serverInstance);

    final Random random = new Random();
    ServerInstance si = serverInstances.remove(random.nextInt(serverInstances.size()));
    si.shutdown();

    System.out.println("Node " + si.getLocalNode().getNodeAddress() + " was removed.");

    System.out.println("[stat] Local maintenance protocol check");
    Thread.sleep(30000);
    checkDataRedistribution(serverInstances.get(0));

    si = new ServerInstance(dbLookup(5), false, true, false);
    si.init();

    System.out.println("[stat] Local maintenance protocol check");
    Thread.sleep(30000);
    checkDataRedistribution(serverInstances.get(0));

    si = serverInstances.remove(random.nextInt(serverInstances.size()));
    si.shutdown();

    System.out.println("Node " + si.getLocalNode().getNodeAddress() + " was removed.");

    System.out.println("[stat] Local maintenance protocol check");
    Thread.sleep(30000);
    checkDataRedistribution(serverInstances.get(0));
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

      final ODatabaseRecord nodeDb = localDHTNode.getDb(null);
      final ODatabaseRecord firstSuccessorDb = localFirstSuccessor.getDb(null);
      final ODatabaseRecord secondSuccessorDb = localSecondSuccessor.getDb(null);

      ODatabaseRingIterator ringIterator = new ODatabaseRingIterator(nodeDb, new ORecordId(1, new OClusterPositionNodeId(dhtNode
          .getPredecessor().getNodeId().add(ONodeId.ONE))), new ORecordId(1, new OClusterPositionNodeId(dhtNode.getNodeAddress()
          .getNodeId())));

      while (ringIterator.hasNext()) {
        final ORecordMetadata recordMetadata = ringIterator.next();

        while (firstSuccessorDb.getRecordMetadata(recordMetadata.getRid()) == null) {
          System.out.println("Wait for record " + recordMetadata.getRid() + " for node " + firstSuccessor.getNodeAddress()
              + " owner " + dhtNode.getNodeAddress());
          Thread.sleep(1000);
        }

        while (secondSuccessorDb.getRecordMetadata(recordMetadata.getRid()) == null) {
          System.out.println("Wait for record " + recordMetadata.getRid() + " for node " + secondSuccessor.getNodeAddress()
              + " owner " + dhtNode.getNodeAddress());
          Thread.sleep(1000);
        }
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

  private int gerOwnRecordsCount(OLocalDHTNode localDHTNode) {
    final ODatabaseRecord nodeDb = localDHTNode.getDb(null);

    ODatabaseRingIterator ringIterator = new ODatabaseRingIterator(nodeDb, new ORecordId(1, new OClusterPositionNodeId(localDHTNode
        .getPredecessor().getNodeId().add(ONodeId.ONE))), new ORecordId(1, new OClusterPositionNodeId(localDHTNode.getNodeAddress()
        .getNodeId())));
    int count = 0;
    while (ringIterator.hasNext()) {
      final ORecordMetadata recordMetadata = ringIterator.next();
      count++;
    }

    return count;
  }

  private ODHTDatabaseLookup dbLookup(int dbNumber) {
    return new ODHTDatabaseLookupImpl("memory:AntiEntropyTest" + dbNumber, "admin", "admin");
  }
}
