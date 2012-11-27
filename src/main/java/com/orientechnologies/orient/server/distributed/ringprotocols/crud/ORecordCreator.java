package com.orientechnologies.orient.server.distributed.ringprotocols.crud;

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.server.distributed.ODHTNodeLocal;
import com.orientechnologies.orient.server.distributed.Record;

/**
 * @author Andrey Lomakin
 * @since 21.11.12
 */
public interface ORecordCreator {
  public Record createRecord(ODHTNodeLocal node, ORID id, String data, int replicaCount, int syncReplicaCount);
}
