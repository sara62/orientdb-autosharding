package com.orientechnologies.orient.server.distributed.ringprotocols;

import com.orientechnologies.orient.core.id.ONodeId;
import com.orientechnologies.orient.server.distributed.ODHTNodeLocal;

/**
 * @author Andrey Lomakin
 * @since 26.11.12
 */
public interface OGlobalMaintenanceProtocol {
	public ONodeId reallocateWrongPlacedReplicas(String storageName, int clusterId, ODHTNodeLocal nodeLocal, ONodeId idToTest, int replicaCount, int syncReplicaCount);
}
