package com.orientechnologies.orient.server.distributed.ringprotocols.crud;

import com.orientechnologies.orient.server.distributed.ODHTNodeLocal;
import com.orientechnologies.orient.server.distributed.util.OWaitTillNodeJoin;
import com.orientechnologies.orient.server.distributed.Record;
import com.orientechnologies.orient.server.distributed.ringprotocols.ORecordReplicator;

/**
 * @author Andrey Lomakin
 * @since 22.11.12
 */
public class ORecordUpdaterImpl implements ORecordUpdater {
	private final ORecordReplicator recordReplicator;
	private final int replicaCount;
	private final int  syncReplicaCount;

	public ORecordUpdaterImpl(ORecordReplicator recordReplicator, int replicaCount, int syncReplicaCount) {
		this.recordReplicator = recordReplicator;
		this.replicaCount = replicaCount;
		this.syncReplicaCount = syncReplicaCount;
	}

	@Override
	public Record updateRecord(ODHTNodeLocal localNode, Record record) {
		OWaitTillNodeJoin.waitTillNodeJoin(localNode);

		localNode.updateData(record.getId(), record);

		recordReplicator.replicateRecord(localNode, record.getId(), replicaCount, syncReplicaCount);

		return localNode.readData(record.getId());
	}
}