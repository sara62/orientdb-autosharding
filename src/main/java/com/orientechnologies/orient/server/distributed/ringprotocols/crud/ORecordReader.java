package com.orientechnologies.orient.server.distributed.ringprotocols.crud;

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.server.distributed.ODHTNodeLocal;
import com.orientechnologies.orient.server.distributed.Record;

/**
 * @author Andrey Lomakin
 * @since 22.11.12
 */
public interface ORecordReader {
	public Record readRecord(ODHTNodeLocal nodeLocal, ORID recordId, int replicaCount, int syncReplicaCount);
}
