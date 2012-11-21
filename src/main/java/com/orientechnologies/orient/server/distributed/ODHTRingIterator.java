package com.orientechnologies.orient.server.distributed;

import java.util.Iterator;
import java.util.NavigableMap;

import com.orientechnologies.orient.core.id.OClusterPositionNodeId;
import com.orientechnologies.orient.core.id.ONodeId;
import com.orientechnologies.orient.core.id.ORecordId;

/**
 * @author Andrey Lomakin
 * @since 12.10.12
 */
public class ODHTRingIterator implements Iterator<RecordMetadata> {
  private final NavigableMap<ORecordId, Record> db;
  private final ORecordId                       start;
  private final ORecordId                       end;

  private Iterator<Record>                      currentIterator;

  private ORecordId                             currentIntervalStart;
  private ORecordId                             currentIntervalEnd;

  public ODHTRingIterator(NavigableMap<ORecordId, Record> db, ORecordId start, ORecordId end) {
    this.db = db;
    this.start = start;
    this.end = end;

    if (end.compareTo(start) > 0) {
      currentIntervalStart = start;
      currentIntervalEnd = end;
    } else {
      currentIntervalStart = start;
      currentIntervalEnd = new ORecordId(start.clusterId, new OClusterPositionNodeId(ONodeId.MAX_VALUE));
    }

    currentIterator = db.subMap(currentIntervalStart, true, currentIntervalEnd, true).values().iterator();
  }

  @Override
  public boolean hasNext() {
    if (currentIntervalEnd == end)
      return currentIterator.hasNext();

    if (currentIterator.hasNext())
      return true;

    currentIntervalStart = new ORecordId(start.clusterId, new OClusterPositionNodeId(ONodeId.MIN_VALUE));
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
