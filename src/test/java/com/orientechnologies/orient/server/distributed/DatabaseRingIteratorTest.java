package com.orientechnologies.orient.server.distributed;

import java.util.TreeMap;

import com.orientechnologies.orient.core.id.ORecordId;
import org.testng.Assert;
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
  public void continuousIntervalTest() {
    final TreeMap<ORID, Record> map = new TreeMap<ORID, Record>();

    for (long i = 0; i < 100; i++) {
      final ORID recordId = convertTORID(i);
      map.put(recordId, new Record(recordId, i + ""));
    }

    final ODatabaseRingIterator ringIterator = new ODatabaseRingIterator(map, convertTORID(20), convertTORID(30));

    for (long i = 20; i <= 30; i++) {
      Assert.assertTrue(ringIterator.hasNext());

      final ORID recordId = convertTORID(i);

      final Record record = map.get(recordId);

      Assert.assertEquals(ringIterator.next(), new ORecordMetadata(recordId, record.getVersion()));
    }

    Assert.assertTrue(!ringIterator.hasNext());
  }

  public void overlappingTest() {
    final TreeMap<ORID, Record> map = new TreeMap<ORID, Record>();

    for (long i = 0; i < 100; i++) {
      final ORID recordId = convertTORID(i);
      map.put(recordId, new Record(recordId, i + ""));
    }

    final ODatabaseRingIterator ringIterator = new ODatabaseRingIterator(map, convertTORID(25), convertTORID(22));

    for (long i = 25; i < 100; i++) {
      Assert.assertTrue(ringIterator.hasNext());

      final Record record = map.get(convertTORID(i));

      Assert.assertEquals(ringIterator.next(), new ORecordMetadata(convertTORID(i), record.getVersion()));
    }

    for (long i = 0; i <= 22; i++) {
      Assert.assertTrue(ringIterator.hasNext());

      final Record record = map.get(convertTORID(i));

      Assert.assertEquals(ringIterator.next(), new ORecordMetadata(convertTORID(i), record.getVersion()));
    }

    Assert.assertTrue(!ringIterator.hasNext());
  }

  private ORID convertTORID(long i) {
    return new ORecordId(1, new OClusterPositionNodeId(ONodeId.valueOf(i)));
  }

}
