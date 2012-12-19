package com.orientechnologies.orient.server.distributed;

import java.util.Iterator;
import java.util.NavigableMap;

import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.id.OClusterPositionNodeId;
import com.orientechnologies.orient.core.id.ONodeId;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecordInternal;

/**
 * @author Andrey Lomakin
 * @since 12.10.12
 */
public final class ODatabaseRingIterator implements Iterator<ORecordMetadata> {
  private final ODatabaseRecord                 db;
  private final ORID 														start;
  private final ORID                            end;

  private Iterator<ORecordInternal<?>>          currentIterator;

  private ORID                                  currentIntervalStart;
  private ORID                                  currentIntervalEnd;

  public ODatabaseRingIterator(ODatabaseRecord db, ORID start, ORID end) {
    this.db = db;
    this.start = start;
    this.end = end;

    if (end.compareTo(start) > 0) {
      currentIntervalStart = start;
      currentIntervalEnd = end;
    } else {
      currentIntervalStart = start;
      currentIntervalEnd = new ORecordId(start.getClusterId(), new OClusterPositionNodeId(ONodeId.MAX_VALUE));
    }

    currentIterator = db.browseCluster().subMap(currentIntervalStart, true, currentIntervalEnd, true).values().iterator();
  }

  @Override
  public boolean hasNext() {
    if (currentIntervalEnd.equals(end))
      return currentIterator.hasNext();

    if (currentIterator.hasNext())
      return true;

    currentIntervalStart = new ORecordId(start.getClusterId(), new OClusterPositionNodeId(ONodeId.ZERO));
    currentIntervalEnd = end;

    currentIterator = db.subMap(currentIntervalStart, true, currentIntervalEnd, true).values().iterator();

    return currentIterator.hasNext();
  }

  @Override
  public ORecordMetadata next() {
    final Record record = currentIterator.next();

    return new ORecordMetadata(record.getId(), record.getVersion());
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }
}
