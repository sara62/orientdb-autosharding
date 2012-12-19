package com.orientechnologies.orient.server.distributed.ringprotocols;

import com.orientechnologies.orient.server.distributed.ODHTNodeLocal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Andrey Lomakin
 * @since 23.11.12
 */
public final class OLocalMaintenanceProtocolRunnable implements Runnable {
	private static final Logger LOGGER = LoggerFactory.getLogger(OLocalMaintenanceProtocolRunnable.class);


	private final ODHTNodeLocal localDHTNode;

	private final int replicaCount;
	private final int syncReplicaCount;

	private final OLocalMaintenanceProtocol localMaintenanceProtocol;

	private final String storageName;
	private final int    clusterId;

	public OLocalMaintenanceProtocolRunnable(ODHTNodeLocal localDHTNode, String storageName, int clusterId,
																					 int replicaCount, int syncReplicaCount,
																					 OLocalMaintenanceProtocol localMaintenanceProtocol) {
		this.localDHTNode = localDHTNode;
		this.storageName = storageName;
		this.clusterId = clusterId;
		this.replicaCount = replicaCount;
		this.syncReplicaCount = syncReplicaCount;
		this.localMaintenanceProtocol = localMaintenanceProtocol;
	}

	@Override
	public void run() {
		try {
			localMaintenanceProtocol.synchronizeReplicasBetweenHolders(localDHTNode, storageName, clusterId, replicaCount, syncReplicaCount);
		} catch (Exception e) {
			LOGGER.error("Error during execution of local maintenance protocol.", e);
		}
	}
}