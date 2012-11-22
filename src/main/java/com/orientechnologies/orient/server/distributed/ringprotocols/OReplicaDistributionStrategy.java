package com.orientechnologies.orient.server.distributed.ringprotocols;

import com.orientechnologies.orient.server.distributed.ONodeAddress;

import java.util.Set;

/**
 * @author Andrey Lomakin
 * @since 22.11.12
 */
public interface OReplicaDistributionStrategy {
	public Set<ONodeAddress>[] chooseReplicas(ONodeAddress[] successors, int replicaCount, int syncReplicaCount);
}
