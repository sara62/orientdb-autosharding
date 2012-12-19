package com.orientechnologies.orient.server.distributed.ringprotocols.crud;

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.server.distributed.ODHTNodeLocal;
import com.orientechnologies.orient.server.distributed.Record;

/**
 * @author Andrey Lomakin
 * @since 21.11.12
 */
public interface ORecordCreator {
  public ORecordInternal<?> createRecord(String storageName, ODHTNodeLocal node, ORecordInternal<?> recordInternal, int replicaCount, int syncReplicaCount);
}
