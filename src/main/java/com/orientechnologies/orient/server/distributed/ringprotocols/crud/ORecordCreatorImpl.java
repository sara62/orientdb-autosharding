package com.orientechnologies.orient.server.distributed.ringprotocols.crud;

import com.orientechnologies.orient.server.distributed.ODHTNodeLocal;
import com.orientechnologies.orient.server.distributed.util.OWaitTillNodeJoin;
import com.orientechnologies.orient.server.distributed.Record;
import com.orientechnologies.orient.server.distributed.ringprotocols.ORecordReplicator;

import com.orientechnologies.orient.core.id.ORID;

/**
 * @author Andrey Lomakin
 * @since 21.11.12
 */
public class ORecordCreatorImpl implements ORecordCreator {
	private final ORecordReplicator recordReplicator;
	private final int replicaCount;
	private final int syncReplicaCount;

	public ORecordCreatorImpl(ORecordReplicator recordReplicator, int replicaCount, int syncReplicaCount) {
		this.recordReplicator = recordReplicator;
		this.replicaCount = replicaCount;
		this.syncReplicaCount = syncReplicaCount;
	}

	@Override
	public Record createRecord(ODHTNodeLocal node, ORID recordId, String data) {
		OWaitTillNodeJoin.waitTillNodeJoin(node);

		final Record result = node.addRecordLocal(recordId, data);
		recordReplicator.replicateRecord(node, recordId, replicaCount, syncReplicaCount);

		return result;
	}
}
