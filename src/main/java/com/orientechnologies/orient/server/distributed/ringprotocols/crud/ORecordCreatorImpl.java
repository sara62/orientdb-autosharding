package com.orientechnologies.orient.server.distributed.ringprotocols.crud;

import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.server.distributed.ODHTNodeLocal;
import com.orientechnologies.orient.server.distributed.util.OWaitTillNodeJoin;
import com.orientechnologies.orient.server.distributed.ringprotocols.ORecordReplicator;

/**
 * @author Andrey Lomakin
 * @since 21.11.12
 */
public final class ORecordCreatorImpl implements ORecordCreator {
	private final ORecordReplicator recordReplicator;

	public ORecordCreatorImpl(ORecordReplicator recordReplicator) {
		this.recordReplicator = recordReplicator;
	}

	@Override
	public ORecordInternal<?> createRecord(String storageName, ODHTNodeLocal node, ORecordInternal<?> recordInternal,
																				 int replicaCount, int syncReplicaCount) {
		OWaitTillNodeJoin.waitTillNodeJoin(node);

		final ORecordInternal<?> result = node.addRecordLocal(storageName, recordInternal);
		recordReplicator.replicateRecord(node, storageName, recordInternal.getIdentity(), replicaCount, syncReplicaCount);

		return result;
	}
}
