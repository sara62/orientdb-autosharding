package com.orientechnologies.orient.server.distributed.ringprotocols;

import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.server.distributed.ODHTNode;
import com.orientechnologies.orient.server.distributed.ODHTNodeLookup;
import com.orientechnologies.orient.server.distributed.ONodeAddress;
import com.orientechnologies.orient.server.distributed.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

/**
 * @author Andrey Lomakin
 * @since 22.11.12
 */
public class ORecordReplicatorImpl implements ORecordReplicator {
	private static final Logger LOGGER = LoggerFactory.getLogger(ORecordReplicatorImpl.class);

	private final ODHTNodeLookup nodeLookup;
	private final OReplicaDistributionStrategy replicaDistributionStrategy;

	public ORecordReplicatorImpl(ODHTNodeLookup nodeLookup, OReplicaDistributionStrategy replicaDistributionStrategy) {
		this.nodeLookup = nodeLookup;
		this.replicaDistributionStrategy = replicaDistributionStrategy;
	}

	@Override
	public void replicateRecord(ODHTNode node, ORecordId recordId, int replicaCount, int syncReplicaCount) {
		if (replicaCount < 1)
			return;

		LOGGER.debug("Replication of record {} replica count is {}", recordId, replicaCount);

		ONodeAddress[] successors = node.getSuccessors();

		LOGGER.debug("Replica holders for record {} are {}", recordId, successors);

		if (successors == null || successors.length == 0)
			return;

		final Set<ONodeAddress>[] replicas =
						replicaDistributionStrategy.chooseReplicas(successors, replicaCount, syncReplicaCount);
		final Set<ONodeAddress> syncReplicas = replicas[0];
		final Set<ONodeAddress> asyncReplicas = replicas[1];

		LOGGER.debug("Async replica holders for record {} are {}", recordId, asyncReplicas);
		LOGGER.debug("Sync replica holders for record {} are {}", recordId, syncReplicas);

		final Record record = node.getRecordFromNode(recordId, false);

		if (record == null) {
			LOGGER.debug("Record {} is absent in node {}", recordId, node.getNodeAddress());
			return;
		}

		for (ONodeAddress syncReplicaHolderAddress : syncReplicas) {
			final ODHTNode replicaHolderNode = nodeLookup.findById(syncReplicaHolderAddress);
			if (replicaHolderNode == null) {
				LOGGER.error("Replica holder with id " + syncReplicaHolderAddress + " is absent.");
				continue;
			}

			replicateRecord(replicaHolderNode, record, false);
		}

		for (ONodeAddress asyncReplicaHolderAddress : asyncReplicas) {
			final ODHTNode replicaHolderNode = nodeLookup.findById(asyncReplicaHolderAddress);
			if (replicaHolderNode == null) {
				LOGGER.error("Replica holder with id " + asyncReplicaHolderAddress + " is absent.");
				continue;
			}

			replicateRecord(replicaHolderNode, record, true);
		}

		LOGGER.debug("Replication of record {} was finished.", recordId);
	}

	private void replicateRecord(ODHTNode replicaHolderNode, Record record, boolean async) {
		try {
			LOGGER.debug("Replication of record {} with async flag is set to {} to holder {}",
							new Object[]{record.getId(), true, replicaHolderNode.getNodeAddress()});

			replicaHolderNode.updateReplica(record, async);

			LOGGER.debug("Replication of record {} with async flag is set to {} to holder {}  was finished.",
							new Object[]{record.getId(), async, replicaHolderNode.getNodeAddress()});

		} catch (Exception e) {
			LOGGER.error("Exception during replication of record " + record.getId() +
							" to node " + replicaHolderNode.getNodeAddress(), e);
			// ignore
		}
	}
}