package com.orientechnologies.orient.server.distributed;

import java.util.TreeMap;

import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * @author Andrey Lomakin
 * @since 12.10.12
 */
@Test
public class DHTRingIteratorTest {
  public void continiousIntervalTest() {
    final TreeMap<Long, Record> map = new TreeMap<Long, Record>();

    for (long i = 0; i < 100; i++)
      map.put(i, new Record(i, i + ""));

    final ODHTRingIterator ringIterator = new ODHTRingIterator(map, 20, 30);

    for (long i = 20; i <= 30; i++) {
      Assert.assertTrue(ringIterator.hasNext());

      final Record record = map.get(i);

      Assert.assertEquals(ringIterator.next(), new RecordMetadata(i, record.getVersion()));
    }

    Assert.assertTrue(!ringIterator.hasNext());
  }

  public void overlappingTest() {
    final TreeMap<Long, Record> map = new TreeMap<Long, Record>();

    for (long i = 0; i < 100; i++)
      map.put(i, new Record(i, i + ""));

    final ODHTRingIterator ringIterator = new ODHTRingIterator(map, 25, 22);

    for (long i = 25; i < 100; i++) {
      Assert.assertTrue(ringIterator.hasNext());

      final Record record = map.get(i);

      Assert.assertEquals(ringIterator.next(), new RecordMetadata(i, record.getVersion()));
    }

    for (long i = 0; i <= 22; i++) {
      Assert.assertTrue(ringIterator.hasNext());

      final Record record = map.get(i);

      Assert.assertEquals(ringIterator.next(), new RecordMetadata(i, record.getVersion()));
    }

    Assert.assertTrue(!ringIterator.hasNext());
  }

}
