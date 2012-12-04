package com.orientechnologies.orient.server.distributed.ringprotocols;

import com.orientechnologies.orient.core.id.ONodeId;
import com.orientechnologies.orient.server.distributed.ODHTNodeLocal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * @author Andrey Lomakin
 * @since 26.11.12
 */
public final class OGlobalMaintenanceProtocolRunnable implements Runnable {
	private static final Logger LOGGER = LoggerFactory.getLogger(OGlobalMaintenanceProtocolImpl.class);

	private final OGlobalMaintenanceProtocol globalMaintenanceProtocol;

	private final ODHTNodeLocal nodeLocal;
	private final int replicaCount;
	private final int syncReplicaCount;

	private ONodeId idToTest;

	public OGlobalMaintenanceProtocolRunnable(OGlobalMaintenanceProtocol globalMaintenanceProtocol, ODHTNodeLocal nodeLocal, int replicaCount, int syncReplicaCount) {
		this.globalMaintenanceProtocol = globalMaintenanceProtocol;
		this.nodeLocal = nodeLocal;
		this.replicaCount = replicaCount;
		this.syncReplicaCount = syncReplicaCount;
		idToTest = nodeLocal.getNodeAddress().getNodeId();
	}

	@Override
	public void run() {
		try{
			idToTest = globalMaintenanceProtocol.reallocateWrongPlacedReplicas(nodeLocal, idToTest,
							replicaCount, syncReplicaCount);
		} catch (Exception e) {
			LOGGER.error("Error during global maintenance protocol execution.", e);

			idToTest = nodeLocal.getNodeAddress().getNodeId();
		}
	}
}
