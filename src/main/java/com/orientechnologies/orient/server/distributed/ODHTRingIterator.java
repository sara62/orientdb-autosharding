package com.orientechnologies.orient.server.distributed;

import java.util.Iterator;
import java.util.NavigableMap;


/**
 * @author Andrey Lomakin
 * @since 12.10.12
 */
public class ODHTRingIterator implements Iterator<RecordMetadata> {
  private final NavigableMap<Long, Record> db;
  private final long                       start;
  private final long                       end;

  private Iterator<Record>                 currentIterator;

  private long                             currentIntervalStart;
  private long                             currentIntervalEnd;

  public ODHTRingIterator(NavigableMap<Long, Record> db, long start, long end) {
    this.db = db;
    this.start = start;
    this.end = end;

    if (end > start) {
      currentIntervalStart = start;
      currentIntervalEnd = end;
    } else {
      currentIntervalStart = start;
      currentIntervalEnd = Long.MAX_VALUE;
    }

    currentIterator = db.subMap(currentIntervalStart, true, currentIntervalEnd, true).values().iterator();
  }

  @Override
  public boolean hasNext() {
    if (currentIntervalEnd == end)
      return currentIterator.hasNext();

    if (currentIterator.hasNext())
      return true;

    currentIntervalStart = 0;
    currentIntervalEnd = end;

    currentIterator = db.subMap(currentIntervalStart, true, currentIntervalEnd, true).values().iterator();

    return currentIterator.hasNext();
  }

  @Override
  public RecordMetadata next() {
    final Record record = currentIterator.next();

    return new RecordMetadata(record.getId(), record.getVersion());
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }
}
