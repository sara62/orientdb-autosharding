package com.orientechnologies.orient.server.distributed;

/**
 * @author Andrey Lomakin
 * @since 21.11.12
 */
public interface ORecordReplicator {
  public void replicateRecord(Record record);
}
