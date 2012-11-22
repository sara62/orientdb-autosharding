package com.orientechnologies.orient.server.distributed.ringprotocols;

import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.server.distributed.Record;

/**
 * @author Andrey Lomakin
 * @since 21.11.12
 */
public interface ORecordCreator {
  public Record createRecord(String data);

  public Record createRecord(ORecordId id, String data);
}
