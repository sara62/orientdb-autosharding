package com.orientechnologies.orient.server.distributed.ringprotocols;

import com.orientechnologies.orient.server.distributed.ODHTNodeLookup;
import com.orientechnologies.orient.server.distributed.ringprotocols.ORecordReplicator;
import com.orientechnologies.orient.server.distributed.ringprotocols.ORecordSynchronizer;
import com.orientechnologies.orient.server.distributed.ringprotocols.OReplicaDistributionStrategy;

/**
 * @author Andrey Lomakin
 * @since 22.11.12
 */
public interface ORingProtocolsFactory {
 public ORecordReplicator createRecordReplicator(
				 ODHTNodeLookup nodeLookup, OReplicaDistributionStrategy replicaDistributionStrategy);

 public ORecordSynchronizer createRecordSynchronizer(
				 ODHTNodeLookup nodeLookup, OReplicaDistributionStrategy replicaDistributionStrategy);

	public OReplicaDistributionStrategy createReplicaDistributionStrategy();
}
