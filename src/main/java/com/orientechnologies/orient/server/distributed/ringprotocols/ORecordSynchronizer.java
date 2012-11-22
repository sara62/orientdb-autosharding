package com.orientechnologies.orient.server.distributed.ringprotocols;

import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.server.distributed.ODHTNode;
import com.orientechnologies.orient.server.distributed.Record;

/**
 * @author Andrey Lomakin
 * @since 22.11.12
 */
public interface ORecordSynchronizer {
	public void synchronizeSyncReplicas(ODHTNode node, ORecordId recordId, int replicaCount, int syncReplicaCount);

	public void synchronizeReplicas(ODHTNode node, ORecordId recordId, int replicaCount, int syncReplicaCount);
}