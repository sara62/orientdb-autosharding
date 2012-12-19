package com.orientechnologies.orient.server.distributed.ringprotocols;

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.server.distributed.ODHTNode;
import com.orientechnologies.orient.server.distributed.Record;

/**
 * @author Andrey Lomakin
 * @since 21.11.12
 */
public interface ORecordReplicator {
  public void replicateRecord(ODHTNode node, String storageName, ORID recordId, int replicaCount, int syncReplicaCount);
}
