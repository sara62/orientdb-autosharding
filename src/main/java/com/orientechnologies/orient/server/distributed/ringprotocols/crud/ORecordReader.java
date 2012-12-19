package com.orientechnologies.orient.server.distributed.ringprotocols.crud;

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.server.distributed.ODHTNodeLocal;
import com.orientechnologies.orient.server.distributed.Record;

/**
 * @author Andrey Lomakin
 * @since 22.11.12
 */
public interface ORecordReader {
	public ORecordInternal<?> readRecord(String storageName, ODHTNodeLocal nodeLocal, ORID recordId, int replicaCount, int syncReplicaCount);
}
