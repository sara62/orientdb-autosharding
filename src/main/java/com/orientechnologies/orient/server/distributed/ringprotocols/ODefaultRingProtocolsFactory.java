package com.orientechnologies.orient.server.distributed.ringprotocols;

import com.orientechnologies.orient.server.distributed.ODHTNodeLookup;

/**
 * @author Andrey Lomakin
 * @since 22.11.12
 */
public class ODefaultRingProtocolsFactory implements ORingProtocolsFactory {
	@Override
	public ORecordReplicator createRecordReplicator(ODHTNodeLookup nodeLookup, OReplicaDistributionStrategy replicaDistributionStrategy) {
		return new ORecordReplicatorImpl(nodeLookup, replicaDistributionStrategy);
	}

	@Override
	public ORecordSynchronizer createRecordSynchronizer(ODHTNodeLookup nodeLookup, OReplicaDistributionStrategy replicaDistributionStrategy) {
		return new ORecordSynchronizerImpl(nodeLookup, replicaDistributionStrategy);
	}

	@Override
	public OReplicaDistributionStrategy createReplicaDistributionStrategy() {
		return new ODefaultReplicaDistributionStrategy();
	}
}