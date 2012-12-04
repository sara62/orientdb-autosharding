package com.orientechnologies.orient.server.distributed.ringprotocols;

import com.orientechnologies.orient.server.distributed.ONodeAddress;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * @author Andrey Lomakin
 * @since 22.11.12
 */
public final class ODefaultReplicaDistributionStrategy implements OReplicaDistributionStrategy {
	@Override
	public Set<ONodeAddress>[] chooseReplicas(ONodeAddress[] successors, int replicaCount, int syncReplicaCount) {
		final Set<ONodeAddress>[] result = new Set[2];

		replicaCount = Math.min(replicaCount, successors.length);

		if (syncReplicaCount > replicaCount)
			syncReplicaCount = replicaCount;

		if (replicaCount == 0) {
			result[0] = Collections.emptySet();
			result[1] = Collections.emptySet();

			return result;
		}

		final Set<ONodeAddress> syncReplicas = new HashSet<ONodeAddress>();

		int holderIndex = 0;
		while (syncReplicas.size() < syncReplicaCount) {
			syncReplicas.add(successors[holderIndex]);
			holderIndex++;
		}

		syncReplicaCount = syncReplicas.size();

		final Set<ONodeAddress> asyncReplicas = new HashSet<ONodeAddress>();
		while (asyncReplicas.size() + syncReplicaCount < replicaCount) {
			asyncReplicas.add(successors[holderIndex]);
			holderIndex++;
		}

		result[0] = syncReplicas;
		result[1] = asyncReplicas;

		return result;
	}
}