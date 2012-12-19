package com.orientechnologies.orient.server.distributed.ringprotocols.crud;

import java.util.Set;

import com.orientechnologies.orient.core.record.ORecordInternal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.server.distributed.ODHTNode;
import com.orientechnologies.orient.server.distributed.ODHTNodeLocal;
import com.orientechnologies.orient.server.distributed.ODHTNodeLookup;
import com.orientechnologies.orient.server.distributed.ONodeAddress;
import com.orientechnologies.orient.server.distributed.ORecordMetadata;

/**
 * @author Andrey Lomakin
 * @since 22.11.12
 */
public final class ORecordMergeStrategyImpl implements ORecordMergeStrategy {
	private static final Logger LOGGER = LoggerFactory.getLogger(ORecordMergeStrategyImpl.class);

	private final ODHTNodeLookup nodeLookup;

	public ORecordMergeStrategyImpl(ODHTNodeLookup nodeLookup) {
		this.nodeLookup = nodeLookup;
	}

	@Override
	public ORecordMergeExecutionContext mergeReplicaVersions(String storageName, ODHTNodeLocal localNode,
																													 ORID recordId, Set<ONodeAddress> replicaHolders) {
		final ORecordMergeExecutionContext executionContext = new ORecordMergeExecutionContext();

		executionContext.setPrimaryHolder(localNode);
		executionContext.setPrimaryMetadata(localNode.getRecordMetadataFromNode(null, recordId));

		mergeReplicaVersions(storageName, localNode, recordId, replicaHolders, executionContext);
		return executionContext;
	}

	@Override
	public void mergeReplicaVersions(String storageName, ODHTNodeLocal localNode, ORID recordId,
																	 Set<ONodeAddress> replicaHolders,
																	 ORecordMergeExecutionContext executionContext) {
		if (executionContext == null) {
			executionContext = new ORecordMergeExecutionContext();
			executionContext.setPrimaryHolder(localNode);
			executionContext.setPrimaryMetadata(localNode.getRecordMetadataFromNode(null, recordId));
		}

		try {
			for (ONodeAddress holderAddress : replicaHolders) {
				final ODHTNode holderNode = nodeLookup.findById(holderAddress);
				if (holderNode == null)
					continue;

				try {
					final ORecordMetadata nodeMetadata = holderNode.getRecordMetadataFromNode(null, recordId);
					if (executionContext.getPrimaryMetadata() == null) {
						if (nodeMetadata != null) {
							executionContext.getReplicaHoldersToUpdate().add(executionContext.getPrimaryHolder().getNodeAddress());
							executionContext.getReplicaHoldersToUpdate().addAll(executionContext.getProcessedHolders());

							executionContext.setPrimaryMetadata(nodeMetadata);
							executionContext.setPrimaryHolder(holderNode);
						}
					} else {
						if (nodeMetadata != null) {
							final int cp = executionContext.getPrimaryMetadata().getVersion().compareTo(nodeMetadata.getVersion());

							if (cp < 0) {
								executionContext.getReplicaHoldersToUpdate().add(executionContext.getPrimaryHolder().getNodeAddress());
								executionContext.getReplicaHoldersToUpdate().addAll(executionContext.getProcessedHolders());

								executionContext.setPrimaryMetadata(nodeMetadata);
								executionContext.setPrimaryHolder(holderNode);
							} else if (cp > 0) {
								executionContext.getReplicaHoldersToUpdate().add(holderAddress);
							}
						} else {
							executionContext.getReplicaHoldersToUpdate().add(holderAddress);
						}
					}

					executionContext.getProcessedHolders().add(holderAddress);
				} catch (Exception e) {
					// ignore
					LOGGER.error("Exception during synchronization of record " + recordId + " for node " + holderAddress, e);
				}
			}

			if (!executionContext.getReplicaHoldersToUpdate().isEmpty()) {
				final ORecordInternal<?> result;

				if (localNode.getNodeAddress().equals(executionContext.getPrimaryHolder().getNodeAddress()))
					result = localNode.readRecordLocal(storageName, recordId);
				else
					result = executionContext.getPrimaryHolder().getRecordFromNode(storageName, recordId);

				if (result == null)
					return;

				for (ONodeAddress replicaHolderAddress : executionContext.getReplicaHoldersToUpdate()) {
					final ODHTNode replicaHolder = nodeLookup.findById(replicaHolderAddress);
					if (replicaHolder == null)
						continue;
					try {
						replicaHolder.updateReplica(storageName, result, false);
					} catch (Exception e) {
						LOGGER.error(
										"Exception during replication of record with id " + recordId + " for node " + replicaHolder.getNodeAddress(), e);
					}
				}

				executionContext.getReplicaHoldersToUpdate().clear();
			}
		} catch (Exception e) {
			LOGGER.error("Exception during merger of replicas of record with id " + recordId + " for node " + localNode.getNodeAddress(),
							e);
		}
	}
}
