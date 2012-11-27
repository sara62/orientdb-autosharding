package com.orientechnologies.orient.server.distributed.ringprotocols;

import com.orientechnologies.orient.server.distributed.ODHTNodeLookup;
import com.orientechnologies.orient.server.distributed.ringprotocols.crud.ORecordCreator;
import com.orientechnologies.orient.server.distributed.ringprotocols.crud.ORecordCreatorImpl;
import com.orientechnologies.orient.server.distributed.ringprotocols.crud.ORecordDeleter;
import com.orientechnologies.orient.server.distributed.ringprotocols.crud.ORecordDeleterImpl;
import com.orientechnologies.orient.server.distributed.ringprotocols.crud.ORecordMergeStrategy;
import com.orientechnologies.orient.server.distributed.ringprotocols.crud.ORecordMergeStrategyImpl;
import com.orientechnologies.orient.server.distributed.ringprotocols.crud.ORecordReader;
import com.orientechnologies.orient.server.distributed.ringprotocols.crud.ORecordReaderImpl;
import com.orientechnologies.orient.server.distributed.ringprotocols.crud.ORecordReaderWithoutReadRepairImpl;
import com.orientechnologies.orient.server.distributed.ringprotocols.crud.ORecordUpdater;
import com.orientechnologies.orient.server.distributed.ringprotocols.crud.ORecordUpdaterImpl;

/**
 * @author Andrey Lomakin
 * @since 22.11.12
 */
public class ODefaultRingProtocolsFactory implements ORingProtocolsFactory {
	private final boolean useReadRepair;

	public ODefaultRingProtocolsFactory(boolean useReadRepair) {
		this.useReadRepair = useReadRepair;
	}

	@Override
	public ORecordMergeStrategy createRecordMergeStrategy(ODHTNodeLookup nodeLookup) {
		return new ORecordMergeStrategyImpl(nodeLookup);
	}

	@Override
	public ORecordReplicator createRecordReplicator(ODHTNodeLookup nodeLookup) {
		return new ORecordReplicatorImpl(nodeLookup, createReplicaDistributionStrategy());
	}

	@Override
	public OReplicaDistributionStrategy createReplicaDistributionStrategy() {
		return new ODefaultReplicaDistributionStrategy();
	}

	@Override
	public ORecordCreator createRecordCreator(ODHTNodeLookup nodeLookup, int replicaCount, int syncReplicaCount) {
		return new ORecordCreatorImpl(createRecordReplicator(nodeLookup));
	}

	@Override
	public ORecordUpdater createRecordUpdater(ODHTNodeLookup nodeLookup, int replicaCount, int syncReplicaCount) {
		return new ORecordUpdaterImpl(createRecordReplicator(nodeLookup));
	}

	@Override
	public ORecordDeleter createRecordDeleter(ODHTNodeLookup nodeLookup, int replicaCount, int syncReplicaCount) {
		return new ORecordDeleterImpl(createRecordReplicator(nodeLookup));
	}

	@Override
	public ORecordReader createRecordReader(ODHTNodeLookup nodeLookup, int replicaCount, int syncReplicaCount) {
		if(useReadRepair)
			return new ORecordReaderImpl(createReplicaDistributionStrategy(), createRecordMergeStrategy(nodeLookup),
						replicaCount, syncReplicaCount);

		return new ORecordReaderWithoutReadRepairImpl();
	}

	@Override
	public OLocalMaintenanceProtocol createLocalMaintenanceProtocol(ODHTNodeLookup nodeLookup) {
		return new OLocalMaintenanceProtocolImpl(createReplicaDistributionStrategy(), nodeLookup);
	}

	@Override
	public OGlobalMaintenanceProtocol createGlobalMaintenanceProtocol(ODHTNodeLookup nodeLookup) {
		return new OGlobalMaintenanceProtocolImpl(nodeLookup, createReplicaDistributionStrategy());
	}
}