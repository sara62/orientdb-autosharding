package com.orientechnologies.orient.server.distributed;

import java.util.Iterator;
import java.util.NavigableMap;

import com.orientechnologies.orient.core.id.OAutoShardedRecordId;

/**
 * @author Andrey Lomakin
 * @since 12.10.12
 */
public class ODHTRingIterator implements Iterator<RecordMetadata> {
  private final NavigableMap<OAutoShardedRecordId, Record> db;
  private final OAutoShardedRecordId                       start;
  private final OAutoShardedRecordId                       end;

  private Iterator<Record>                                 currentIterator;

  private OAutoShardedRecordId                             currentIntervalStart;
  private OAutoShardedRecordId                             currentIntervalEnd;

  public ODHTRingIterator(NavigableMap<OAutoShardedRecordId, Record> db, OAutoShardedRecordId start, OAutoShardedRecordId end) {
    this.db = db;
    this.start = start;
    this.end = end;

    if (end.compareTo(start) > 0) {
      currentIntervalStart = start;
      currentIntervalEnd = end;
    } else {
      currentIntervalStart = start;
      currentIntervalEnd = ONodeId.convertToRecordId(ONodeId.MAX_VALUE, start.clusterId);
    }

    currentIterator = db.subMap(currentIntervalStart, true, currentIntervalEnd, true).values().iterator();
  }

  @Override
  public boolean hasNext() {
    if (currentIntervalEnd == end)
      return currentIterator.hasNext();

    if (currentIterator.hasNext())
      return true;

    currentIntervalStart = ONodeId.convertToRecordId(ONodeId.MIN_VALUE, start.clusterId);
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
