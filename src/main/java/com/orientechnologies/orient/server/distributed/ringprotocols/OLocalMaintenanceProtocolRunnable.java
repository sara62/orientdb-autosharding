package com.orientechnologies.orient.server.distributed.ringprotocols;

import com.orientechnologies.orient.server.distributed.ODHTNodeLocal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Andrey Lomakin
 * @since 23.11.12
 */
public class OLocalMaintenanceProtocolRunnable implements Runnable {
	private static final Logger LOGGER = LoggerFactory.getLogger(OLocalMaintenanceProtocolRunnable.class);


	private final ODHTNodeLocal localDHTNode;

	private final int replicaCount;
	private final int syncReplicaCount;

	private final OLocalMaintenanceProtocol localMaintenanceProtocol;

	public OLocalMaintenanceProtocolRunnable(ODHTNodeLocal localDHTNode, int replicaCount, int syncReplicaCount,
																					 OLocalMaintenanceProtocol localMaintenanceProtocol) {
		this.localDHTNode = localDHTNode;
		this.replicaCount = replicaCount;
		this.syncReplicaCount = syncReplicaCount;
		this.localMaintenanceProtocol = localMaintenanceProtocol;
	}

	@Override
	public void run() {
		try {
			localMaintenanceProtocol.synchronizeReplicasBetweenHolders(localDHTNode, replicaCount, syncReplicaCount);
		} catch (Exception e) {
			LOGGER.error("Error during execution of local maintenance protocol.", e);
		}
	}
}