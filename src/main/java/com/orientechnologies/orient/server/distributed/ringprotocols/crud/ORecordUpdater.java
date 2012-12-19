package com.orientechnologies.orient.server.distributed.ringprotocols.crud;

import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.server.distributed.ODHTNodeLocal;
import com.orientechnologies.orient.server.distributed.Record;

/**
 * @author Andrey Lomakin
 * @since 22.11.12
 */
public interface ORecordUpdater {
	public ORecordInternal<?> updateRecord(String storageName, ODHTNodeLocal localNode, ORecordInternal<?> record, int replicaCount, int syncReplicaCount);
}
