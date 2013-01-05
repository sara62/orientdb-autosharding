package com.orientechnologies.orient.server.distributed;

import java.util.TreeMap;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseComplex;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.id.OClusterPositionNodeId;
import com.orientechnologies.orient.core.id.ONodeId;
import com.orientechnologies.orient.core.id.ORID;

/**
 * @author Andrey Lomakin
 * @since 12.10.12
 */
@Test
public class DatabaseRingIteratorTest {
  public static final int     CLUSTER_ID = 1;
  private ODatabaseDocumentTx db;
  private ODatabaseDocumentTx db2;

  @BeforeMethod
  public void setUp() {
    OGlobalConfiguration.DB_USE_DISTRIBUTED_VERSION.setValue(true);
    OGlobalConfiguration.USE_NODE_ID_CLUSTER_POSITION.setValue(true);
    OGlobalConfiguration.USE_LHPEPS_MEMORY_CLUSTER.setValue(true);
    OGlobalConfiguration.STORAGE_USE_TOMBSTONES.setValue(true);

    db = new ODatabaseDocumentTx("memory:ringIteratorTest");
    db.create();

    db2 = new ODatabaseDocumentTx("memory:ringIteratorTestSecondaryDb");
    db2.create();
  }

  @AfterMethod
  public void tearDown() throws Exception {
    db.drop();
    db2.drop();
  }


  public void continuousIntervalTest() {
    for (long i = 0; i < 100; i++) {
      final ODocument doc = new ODocument();
      doc.setIdentity(convertTORID(i));
      doc.field("value", i + "");
      db.save(doc, ODatabaseComplex.OPERATION_MODE.SYNCHRONOUS, true, null, null);
    }

    final ODatabaseRingIterator ringIterator = new ODatabaseRingIterator(db, convertTORID(20), convertTORID(30));

    for (long i = 20; i <= 30; i++) {
      Assert.assertTrue(ringIterator.hasNext());

      final ORID recordId = convertTORID(i);

      final ODocument record = db.load(recordId);

      Assert.assertEquals(ringIterator.next(), new ORecordMetadata(recordId, record.getRecordVersion()));
    }

    Assert.assertTrue(!ringIterator.hasNext());
  }

  public void overlappingTest() {
    for (long i = 0; i < 100; i++) {
      final ODocument doc = new ODocument();
      doc.setIdentity(convertTORID(i));
      doc.field("value", i + "");
      db.save(doc, ODatabaseComplex.OPERATION_MODE.SYNCHRONOUS, true, null, null);
    }

    final ODatabaseRingIterator ringIterator = new ODatabaseRingIterator(db, convertTORID(25), convertTORID(22));

    for (long i = 25; i < 100; i++) {
      Assert.assertTrue(ringIterator.hasNext(), "Iteration #" + i);

      final ORecordInternal<?> record = db.load(convertTORID(i));

      Assert.assertEquals(ringIterator.next(), new ORecordMetadata(convertTORID(i), record.getRecordVersion()), "Iteration #" + i);
    }

    for (long i = 0; i <= 22; i++) {
      Assert.assertTrue(ringIterator.hasNext());

      final ORecordInternal<?> record = db.load(convertTORID(i));

      Assert.assertEquals(ringIterator.next(), new ORecordMetadata(convertTORID(i), record.getRecordVersion()));
    }

    Assert.assertTrue(!ringIterator.hasNext());
  }

  private ORecordId convertTORID(long i) {
    return new ORecordId(CLUSTER_ID, new OClusterPositionNodeId(ONodeId.valueOf(i)));
  }

}
