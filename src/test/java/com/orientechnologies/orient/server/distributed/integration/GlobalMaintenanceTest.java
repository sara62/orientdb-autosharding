package com.orientechnologies.orient.server.distributed.integration;

import java.util.ArrayList;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.id.OClusterPositionFactory;
import com.orientechnologies.orient.core.id.OClusterPositionNodeId;
import com.orientechnologies.orient.core.id.ONodeId;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.distributed.ODHTDatabaseLookup;
import com.orientechnologies.orient.server.distributed.ODHTDatabaseLookupImpl;
import com.orientechnologies.orient.server.distributed.ODHTNode;
import com.orientechnologies.orient.server.distributed.ODatabaseRingIterator;
import com.orientechnologies.orient.server.distributed.OLocalDHTNode;
import com.orientechnologies.orient.server.distributed.ORecordMetadata;
import com.orientechnologies.orient.server.hazelcast.OHazelcastDHTNodeProxy;
import com.orientechnologies.orient.server.hazelcast.ServerInstance;

/**
 * @author Andrey Lomakin
 * @since 25.10.12
 */
@Test
public class GlobalMaintenanceTest {
  public static final String    STORAGE_NAME = "storageName";
  public static final int       DB_COUNT     = 4;
  private ODatabaseDocumentTx[] db;
  public int                    clusterId;

  @BeforeMethod
  public void setUp() {
    OGlobalConfiguration.DB_USE_DISTRIBUTED_VERSION.setValue(true);
    OGlobalConfiguration.USE_NODE_ID_CLUSTER_POSITION.setValue(true);
    OGlobalConfiguration.USE_LHPEPS_MEMORY_CLUSTER.setValue(true);
    OGlobalConfiguration.STORAGE_USE_TOMBSTONES.setValue(true);

    db = new ODatabaseDocumentTx[DB_COUNT];
    for (int i = 0; i < DB_COUNT; i++) {
      db[i] = new ODatabaseDocumentTx("memory:GlobalMaintenanceTest" + i);
      db[i].create();
    }

    clusterId = db[0].getClusterIdByName("default");
  }

  @AfterMethod
  public void tearDown() throws Exception {
    for (int i = 0; i < DB_COUNT; i++) {
      db[i].drop();
    }
  }

  public void testNodeReplicaRedistribution() throws Exception {
    System.out.println("[stat] Ring initialization.");

    final List<ServerInstance> serverInstances = new ArrayList<ServerInstance>(5);

    final ServerInstance serverInstance = new ServerInstance(dbLookup(0));
    serverInstance.init();
    serverInstances.add(serverInstance);

    for (int i = 1; i < 3; i++) {
      ServerInstance si = new ServerInstance(dbLookup(i), false, false, true);
      si.init();
      serverInstances.add(si);
    }

    Thread.sleep(10000);
    System.out.println("[stat] Ring was initialized.");
    System.out.println("[stat] Start data filling.");

    for (int i = 0; i < 100000; i++) {

      final ODocument doc = new ODocument();
      doc.setIdentity(generateRid());
      doc.field("value", i + "");

      serverInstance.create("storageName", doc);
    }

    System.out.println("[stat] Data were added.");
    Thread.sleep(5000);

    checkDataRedistribution(serverInstance);

    ServerInstance si = new ServerInstance(dbLookup(3), false, true, false);
    si.init();

    serverInstances.add(si);

    System.out.println("[stat] Global maintenance protocol check");
    Thread.sleep(300000);

    checkDataRedistributionWhenNewNodeWasAdded(serverInstance);

    final OLocalDHTNode localDHTNode = si.getLocalNode();
    final OLocalDHTNode localPredecessorOne = getLocalNode(si.findById(localDHTNode.getPredecessor()));
    final OLocalDHTNode localPredecessorTwo = getLocalNode(si.findById(localPredecessorOne.getPredecessor()));

    int dhtNodeRecords = getOwnRecordsCount(localDHTNode);
    int predecessorOneRecords = getOwnRecordsCount(localPredecessorOne);
    int predecessorTwoRecords = getOwnRecordsCount(localPredecessorTwo);

    Assert
        .assertEquals(localDHTNode.getDb("storageName").getSize(), dhtNodeRecords + predecessorOneRecords + predecessorTwoRecords);

    for (ServerInstance instance : serverInstances) {
      instance.shutdown();
    }
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

      final ODatabaseRecord nodeDb = localDHTNode.getDb(STORAGE_NAME);
      final ODatabaseRecord firstSuccessorDb = localFirstSuccessor.getDb(STORAGE_NAME);
      final ODatabaseRecord secondSuccessorDb = localSecondSuccessor.getDb(STORAGE_NAME);

      ODatabaseRingIterator ringIterator = new ODatabaseRingIterator(nodeDb, new ORecordId(clusterId, new OClusterPositionNodeId(dhtNode
          .getPredecessor().getNodeId().add(ONodeId.ONE))), new ORecordId(clusterId, new OClusterPositionNodeId(dhtNode.getNodeAddress()
          .getNodeId())));

      while (ringIterator.hasNext()) {
        final ORecordMetadata recordMetadata = ringIterator.next();

        Assert.assertNotNull(firstSuccessorDb.getRecordMetadata(recordMetadata.getRid()));
        Assert.assertNotNull(secondSuccessorDb.getRecordMetadata(recordMetadata.getRid()));
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

      final ODatabaseRecord nodeDb = localDHTNode.getDb(STORAGE_NAME);
      final ODatabaseRecord firstSuccessorDb = localFirstSuccessor.getDb(STORAGE_NAME);
      final ODatabaseRecord secondSuccessorDb = localSecondSuccessor.getDb(STORAGE_NAME);
      final ODatabaseRecord thirdSuccessorDb = localThirdSuccessor.getDb(STORAGE_NAME);

      ODatabaseRingIterator ringIterator = new ODatabaseRingIterator(nodeDb, new ORecordId(clusterId, new OClusterPositionNodeId(dhtNode
          .getPredecessor().getNodeId().add(ONodeId.ONE))), new ORecordId(clusterId, new OClusterPositionNodeId(dhtNode.getNodeAddress()
          .getNodeId())));

      while (ringIterator.hasNext()) {
        final ORecordMetadata recordMetadata = ringIterator.next();

        Assert.assertNotNull(firstSuccessorDb.getRecordMetadata(recordMetadata.getRid()));
        Assert.assertNotNull(secondSuccessorDb.getRecordMetadata(recordMetadata.getRid()));

        Assert.assertNull(thirdSuccessorDb.getRecordMetadata(recordMetadata.getRid()));
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
    final ODatabaseRecord nodeDb = localDHTNode.getDb(null);

    ODatabaseRingIterator ringIterator = new ODatabaseRingIterator(nodeDb, new ORecordId(clusterId, new OClusterPositionNodeId(localDHTNode
        .getPredecessor().getNodeId().add(ONodeId.ONE))), new ORecordId(clusterId, new OClusterPositionNodeId(localDHTNode.getNodeAddress()
        .getNodeId())));

    int count = 0;
    while (ringIterator.hasNext()) {
      final ORecordMetadata recordMetadata = ringIterator.next();
      count++;
    }

    return count;
  }

  private ODHTDatabaseLookup dbLookup(int dbNumber) {
    return new ODHTDatabaseLookupImpl("memory:GlobalMaintenanceTest" + dbNumber, "admin", "admin");
  }

  private ORecordId generateRid() {
    return new ORecordId(clusterId, OClusterPositionFactory.INSTANCE.generateUniqueClusterPosition());
  }
}
