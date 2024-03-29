package com.orientechnologies.orient.server.distributed.ringprotocols;

import com.orientechnologies.orient.server.distributed.ODHTNodeLocal;

/**
 * @author Andrey Lomakin
 * @since 23.11.12
 */
public interface OLocalMaintenanceProtocol {
	public void synchronizeReplicasBetweenHolders(ODHTNodeLocal localDHTNode, String storageName, int clusterId, int replicaCount, int syncReplicaCount);
}
