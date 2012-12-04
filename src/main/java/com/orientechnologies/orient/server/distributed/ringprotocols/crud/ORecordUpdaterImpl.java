package com.orientechnologies.orient.server.distributed.ringprotocols.crud;

import com.orientechnologies.orient.server.distributed.ODHTNodeLocal;
import com.orientechnologies.orient.server.distributed.util.OWaitTillNodeJoin;
import com.orientechnologies.orient.server.distributed.Record;
import com.orientechnologies.orient.server.distributed.ringprotocols.ORecordReplicator;

/**
 * @author Andrey Lomakin
 * @since 22.11.12
 */
public final class ORecordUpdaterImpl implements ORecordUpdater {
	private final ORecordReplicator recordReplicator;

	public ORecordUpdaterImpl(ORecordReplicator recordReplicator) {
		this.recordReplicator = recordReplicator;
	}

	@Override
	public Record updateRecord(ODHTNodeLocal localNode, Record record, int replicaCount, int syncReplicaCount) {
		OWaitTillNodeJoin.waitTillNodeJoin(localNode);

		localNode.updateRecordLocal(record.getId(), record);

		recordReplicator.replicateRecord(localNode, record.getId(), replicaCount, syncReplicaCount);

		return localNode.readRecordLocal(record.getId());
	}
}