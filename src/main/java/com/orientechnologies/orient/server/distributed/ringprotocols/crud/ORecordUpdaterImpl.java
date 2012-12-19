package com.orientechnologies.orient.server.distributed.ringprotocols.crud;

import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.server.distributed.ODHTNodeLocal;
import com.orientechnologies.orient.server.distributed.util.OWaitTillNodeJoin;
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
	public ORecordInternal<?> updateRecord(String storageName, ODHTNodeLocal localNode,
																				 ORecordInternal<?> record, int replicaCount, int syncReplicaCount) {
		OWaitTillNodeJoin.waitTillNodeJoin(localNode);

		localNode.updateRecordLocal(storageName, record);

		recordReplicator.replicateRecord(localNode, storageName, record.getIdentity(), replicaCount, syncReplicaCount);

		return localNode.readRecordLocal(storageName, record.getIdentity());
	}
}