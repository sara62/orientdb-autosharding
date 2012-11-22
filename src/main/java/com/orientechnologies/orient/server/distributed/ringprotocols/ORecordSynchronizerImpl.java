package com.orientechnologies.orient.server.distributed.ringprotocols;

import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.server.distributed.ODHTNode;
import com.orientechnologies.orient.server.distributed.ODHTNodeLookup;
import com.orientechnologies.orient.server.distributed.ONodeAddress;
import com.orientechnologies.orient.server.distributed.Record;
import com.orientechnologies.orient.server.distributed.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

/**
 * @author Andrey Lomakin
 * @since 22.11.12
 */
public class ORecordSynchronizerImpl implements ORecordSynchronizer {
	private static final Logger LOGGER = LoggerFactory.getLogger(ORecordSynchronizerImpl.class);

	private final ODHTNodeLookup nodeLookup;
	private final OReplicaDistributionStrategy replicaDistributionStrategy;

	public ORecordSynchronizerImpl(ODHTNodeLookup nodeLookup, OReplicaDistributionStrategy replicaDistributionStrategy) {
		this.nodeLookup = nodeLookup;
		this.replicaDistributionStrategy = replicaDistributionStrategy;
	}

	@Override
	public void synchronizeSyncReplicas(ODHTNode node, ORecordId recordId, int replicaCount, int syncReplicaCount) {
		if (syncReplicaCount < 1)
			return;

		LOGGER.debug("Start synchronization  sync replica count is {}",	syncReplicaCount);

		while (true) {
			LOGGER.debug("Find successors for node {} replica count is {}", node.getNodeAddress(), replicaCount);

			final ONodeAddress[] successors = node.getSuccessors();
			LOGGER.debug("Replica successors for node {} are {}", node.getNodeAddress(), successors);

			if (successors == null || successors.length == 0)
				return;

			final Set<ONodeAddress> holdersToReplicate =
							replicaDistributionStrategy.chooseReplicas(successors, replicaCount, syncReplicaCount)[0];

			if (!synchronizeRecord(node, recordId, holdersToReplicate))
				continue;

			return;
		}
	}

	@Override
	public void synchronizeReplicas(ODHTNode node, ORecordId recordId, int replicaCount, int syncReplicaCount) {
		if (replicaCount < 1)
			return;

		LOGGER.debug("Start synchronization replica count is {}",	replicaCount);

		while (true) {
			LOGGER.debug("Find successors for node {} replica count is {}", node.getNodeAddress(), replicaCount);

			final ONodeAddress[] successors = node.getSuccessors();
			LOGGER.debug("Replica successors for node {} are {}", node.getNodeAddress(), successors);

			if (successors == null || successors.length == 0)
				return;

			final Set<ONodeAddress>[] replicaHolders =
							replicaDistributionStrategy.chooseReplicas(successors, replicaCount, syncReplicaCount);

			final Set<ONodeAddress> holdersToReplicate = new HashSet<ONodeAddress>(replicaHolders[0]);
			holdersToReplicate.addAll(replicaHolders[1]);

			if (!synchronizeRecord(node, recordId, holdersToReplicate))
				continue;

			return;
		}
	}

	private boolean synchronizeRecord(ODHTNode node, ORecordId recordId, Set<ONodeAddress> holdersToReplicate) {
		ODHTNode primaryHolder = node;
		final Set<ONodeAddress> replicaHoldersToUpdate = new HashSet<ONodeAddress>();

		RecordMetadata primaryMetadata;

		Record record = node.getRecordFromNode(recordId, false);
		if (record == null)
			primaryMetadata = null;
		else
			primaryMetadata = new RecordMetadata(record.getId(), record.getVersion());


		LOGGER.debug("Replica holders for node {} are {}", node.getNodeAddress(), replicaHoldersToUpdate);

		for (ONodeAddress holderId : holdersToReplicate) {
			final ODHTNode holderNode = nodeLookup.findById(holderId);
			if (holderNode == null)
				continue;

			LOGGER.debug("Holder with id {} is absent.", holderId);

			try {
				final RecordMetadata nodeMetadata = holderNode.getRecordMetadataFromNode(recordId);

				LOGGER.debug("Metadata for record id {} from node {} is {}", new Object[] { recordId, holderId, nodeMetadata });

				if (primaryMetadata == null) {
					if (nodeMetadata != null) {
						LOGGER.debug(
										"Primary metadata is null but node metadata is not so we replace it. record id {} node metadata {}",
										recordId, nodeMetadata);

						replicaHoldersToUpdate.add(primaryHolder.getNodeAddress());

						primaryMetadata = nodeMetadata;
						primaryHolder = holderNode;
					}
				} else {
					if (nodeMetadata != null) {
						final int cp = primaryMetadata.getVersion().compareTo(nodeMetadata.getVersion());

						if (cp < 0) {
							replicaHoldersToUpdate.add(primaryHolder.getNodeAddress());

							primaryMetadata = nodeMetadata;
							primaryHolder = holderNode;

							LOGGER.debug("Primary metadata is not null but node metadata is more up to date  so we replace it."
											+ " record id {} node metadata {} primary metadata {}",
											new Object[] { recordId, nodeMetadata, primaryMetadata });
						} else if (cp > 0) {
							LOGGER.debug("Primary metadata is not null but node metadata out of date so we replace it. record id {}"
											+ " node metadata {} primary metadata {}", new Object[] { recordId, nodeMetadata, primaryMetadata });

							replicaHoldersToUpdate.add(holderNode.getNodeAddress());
						}
					} else {
						LOGGER.debug("Node metadata is null but primary metadata is not so we replace it. record id {}"
										+ " node metadata {} primary metadata {}", new Object[] { recordId, nodeMetadata, primaryMetadata });

						replicaHoldersToUpdate.add(holderNode.getNodeAddress());
					}
				}
			} catch (Exception e) {
				// ignore
				LOGGER.error("Exception during synchronization of record " + recordId + " for node " + holderId, e);
			}
		}

		LOGGER.debug("Replica holders to update for record {}", recordId);

		if (!replicaHoldersToUpdate.isEmpty()) {
			LOGGER.debug("Getting record from {} with id {}", primaryHolder.getNodeAddress(), recordId);

			Record result;
			if (node.getNodeAddress().equals(primaryHolder.getNodeAddress()))
				result = record;
			else
				try {
					result = primaryHolder.getRecordFromNode(recordId, false);
				} catch (Exception e) {
					return false;
				}

			LOGGER.debug("Record with id {} was returned from {}" + " with content {}" + result,
							new Object[] { recordId, primaryHolder.getNodeAddress(), result });

			for (ONodeAddress replicaHolderAddress : replicaHoldersToUpdate) {
				final ODHTNode replicaHolder = nodeLookup.findById(replicaHolderAddress);
				if (replicaHolder == null)
					continue;

				LOGGER.debug("Holder with id {} is absent during synchronization of record with id {}", replicaHolderAddress,
								recordId);
				try {
					LOGGER.debug("Update of replica with {} for node {}", recordId, replicaHolder.getNodeAddress());
					replicaHolder.updateReplica(result, false);
					LOGGER.debug("Replica with {} for node {} was updated.", recordId, replicaHolder.getNodeAddress());
				} catch (Exception e) {
					LOGGER.error(
									"Exception during replication of record with id " + recordId + " for node " + replicaHolder.getNodeAddress(), e);
				}
			}
		}

		LOGGER.debug("Synchronization of record with id {} was completed", recordId);

		return true;
	}
}