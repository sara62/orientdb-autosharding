package com.orientechnologies.orient.server.distributed.ringprotocols.crud;

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.server.distributed.ODHTNodeLocal;
import com.orientechnologies.orient.server.distributed.util.OWaitTillNodeJoin;

/**
 * @author Andrey Lomakin
 * @since 23.11.12
 */
public final class ORecordReaderWithoutReadRepairImpl implements ORecordReader {
	public ORecordReaderWithoutReadRepairImpl() {
	}

	@Override
	public ORecordInternal<?> readRecord(String storageName, ODHTNodeLocal nodeLocal, ORID recordId, int replicaCount, int syncReplicaCount) {
		OWaitTillNodeJoin.waitTillNodeJoin(nodeLocal);
		return nodeLocal.readRecordLocal(storageName, recordId);
	}
}