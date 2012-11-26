package com.orientechnologies.orient.server.distributed;

import java.util.TreeMap;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.id.OClusterPositionNodeId;
import com.orientechnologies.orient.core.id.ONodeId;
import com.orientechnologies.orient.core.id.ORecordId;

/**
 * @author Andrey Lomakin
 * @since 12.10.12
 */
@Test
public class DatabaseRingIteratorTest {
  public void continuousIntervalTest() {
    final TreeMap<ORecordId, Record> map = new TreeMap<ORecordId, Record>();

    for (long i = 0; i < 100; i++) {
      final ORecordId recordId = convertToRecordId(i);
      map.put(recordId, new Record(recordId, i + ""));
    }

    final ODatabaseRingIterator ringIterator = new ODatabaseRingIterator(map, convertToRecordId(20), convertToRecordId(30));

    for (long i = 20; i <= 30; i++) {
      Assert.assertTrue(ringIterator.hasNext());

      final ORecordId recordId = convertToRecordId(i);

      final Record record = map.get(recordId);

      Assert.assertEquals(ringIterator.next(), new RecordMetadata(recordId, record.getVersion()));
    }

    Assert.assertTrue(!ringIterator.hasNext());
  }

  public void overlappingTest() {
    final TreeMap<ORecordId, Record> map = new TreeMap<ORecordId, Record>();

    for (long i = 0; i < 100; i++) {
      final ORecordId recordId = convertToRecordId(i);
      map.put(recordId, new Record(recordId, i + ""));
    }

    final ODatabaseRingIterator ringIterator = new ODatabaseRingIterator(map, convertToRecordId(25), convertToRecordId(22));

    for (long i = 25; i < 100; i++) {
      Assert.assertTrue(ringIterator.hasNext());

      final Record record = map.get(convertToRecordId(i));

      Assert.assertEquals(ringIterator.next(), new RecordMetadata(convertToRecordId(i), record.getVersion()));
    }

    for (long i = 0; i <= 22; i++) {
      Assert.assertTrue(ringIterator.hasNext());

      final Record record = map.get(convertToRecordId(i));

      Assert.assertEquals(ringIterator.next(), new RecordMetadata(convertToRecordId(i), record.getVersion()));
    }

    Assert.assertTrue(!ringIterator.hasNext());
  }

  private ORecordId convertToRecordId(long i) {
    return new ORecordId(1, new OClusterPositionNodeId(ONodeId.valueOf(i)));
  }

}
