package com.orientechnologies.orient.server.distributed;

import com.orientechnologies.orient.core.id.ORecordId;

/**
 * @author Andrey Lomakin
 * @since 21.11.12
 */
public interface ORecordCreator {
  public Record createRecord(String data);

  public Record createRecord(ORecordId id, String data);
}
