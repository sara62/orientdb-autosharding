package com.orientechnologies.orient.server.distributed.ringprotocols;

import com.orientechnologies.orient.core.id.OClusterPositionNodeId;
import com.orientechnologies.orient.core.id.ONodeId;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.server.distributed.ODHTNode;
import com.orientechnologies.orient.server.distributed.ODHTNodeLocal;
import com.orientechnologies.orient.server.distributed.ODHTNodeLookup;
import com.orientechnologies.orient.server.distributed.ODHTRingInterval;
import com.orientechnologies.orient.server.distributed.ONodeAddress;
import com.orientechnologies.orient.server.distributed.Record;
import com.orientechnologies.orient.server.distributed.RecordMetadata;
import com.orientechnologies.orient.server.distributed.merkletree.ODetachedMerkleTreeNode;
import com.orientechnologies.orient.server.distributed.merkletree.OMerkleTree;
import com.orientechnologies.orient.server.distributed.util.OWaitTillNodeJoin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
* @author Andrey Lomakin
* @since 23.11.12
*/
public final class OLocalMaintenanceProtocolImpl implements OLocalMaintenanceProtocol {
	private final Logger logger = LoggerFactory.getLogger(OLocalMaintenanceProtocolImpl.class);

	private final OReplicaDistributionStrategy replicaDistributionStrategy;
	private final ODHTNodeLookup nodeLookup;

	public OLocalMaintenanceProtocolImpl(OReplicaDistributionStrategy replicaDistributionStrategy,
																			 ODHTNodeLookup nodeLookup) {
		this.replicaDistributionStrategy = replicaDistributionStrategy;
		this.nodeLookup = nodeLookup;
	}

	@Override
	public void synchronizeReplicasBetweenHolders(final ODHTNodeLocal localDHTNode, int replicaCount, int syncReplicaCount) {
		OWaitTillNodeJoin.waitTillNodeJoin(localDHTNode);

		final ONodeAddress localPredecessor = localDHTNode.getPredecessor();

		if (localPredecessor == null)
			return;

		final Set<ONodeAddress> replicaHolderAddresses = new HashSet<ONodeAddress>();
		final Set<ONodeAddress>[] replicaHolders = replicaDistributionStrategy.chooseReplicas(localDHTNode.getSuccessors(),
						replicaCount, syncReplicaCount);

		replicaHolderAddresses.addAll(replicaHolders[0]);
		replicaHolderAddresses.addAll(replicaHolders[1]);

		for (ONodeAddress replicaHolderAddress : replicaHolderAddresses) {
			try {
				final ONodeId startId = localPredecessor.getNodeId().add(ONodeId.ONE);
				final ONodeId endId = localDHTNode.getNodeAddress().getNodeId();

				final List<ODetachedMerkleTreeNode> roots = getRootsForInterval(localDHTNode.getLocalMerkleTree(), startId, endId);
				for (final ODetachedMerkleTreeNode rootNode : roots)
					synchronizeNode(localDHTNode, rootNode, replicaHolderAddress);
			} catch (Exception e) {
				logger.error("Error during replication of content to node " + replicaHolderAddress, e);
			}
		}
	}

	private List<ODetachedMerkleTreeNode> getRootsForInterval(final OMerkleTree tree, final ONodeId startId, final ONodeId endId) {
		if (startId.compareTo(endId) <= 0)
			return tree.getRootNodesForInterval(startId, endId);

		final List<ODetachedMerkleTreeNode> result = new ArrayList<ODetachedMerkleTreeNode>();

		final List<ODetachedMerkleTreeNode> firstInterval = tree.getRootNodesForInterval(startId, ONodeId.MAX_VALUE);
		final List<ODetachedMerkleTreeNode> secondInterval = tree.getRootNodesForInterval(ONodeId.ZERO, endId);

		result.addAll(firstInterval);

		if (!secondInterval.isEmpty() && !firstInterval.isEmpty()
				&& Arrays.equals(secondInterval.get(0).getHash(), firstInterval.get(firstInterval.size() - 1).getHash())) {
			if (secondInterval.size() > 1)
				result.addAll(secondInterval.subList(1, secondInterval.size()));
		} else {
			result.addAll(secondInterval);
		}

		return result;
	}

	private void synchronizeNode(final ODHTNodeLocal localDHTNode,
															 final ODetachedMerkleTreeNode localTreeNode, final ONodeAddress remoteNodeAddress) {
		if (localTreeNode == null)
			throw new ONodeSynchronizationFailedException("Passed Local Merkle Tree node is null.");

		final ODHTNode remoteNode = nodeLookup.findById(remoteNodeAddress);

		if (remoteNode == null)
			throw new ONodeSynchronizationFailedException("Node with id " + remoteNodeAddress + " is absent.");

		final ODetachedMerkleTreeNode remoteTreeNode = remoteNode.findMerkleTreeNode(localTreeNode);

		if (remoteTreeNode == null)
			throw new ONodeSynchronizationFailedException("Related remote Merkle tree node is null.");

		compareNodes(localDHTNode, localTreeNode, remoteTreeNode, remoteNodeAddress);

		final ONodeAddress localPredecessor = localDHTNode.getPredecessor();
		if (localPredecessor == null)
			throw new ONodeSynchronizationFailedException("Predecessor " + localPredecessor + " is absent.");

		final ODHTRingInterval nodeInterval = new ODHTRingInterval(localPredecessor.getNodeId().add(ONodeId.ONE),
				localDHTNode.getNodeAddress().getNodeId());

		if (!localTreeNode.isLeaf() && !remoteTreeNode.isLeaf()) {
			for (int i = 0; i < 64; i++) {

				final ODetachedMerkleTreeNode childTreeNode = localDHTNode.getLocalMerkleTree().getChildNode(localTreeNode, i);
				final ONodeId startNodeId = childTreeNode.getStartId();
				final ONodeId endNodeId = childTreeNode.getEndId();

				final ODHTRingInterval treeNodeInterval = new ODHTRingInterval(startNodeId, endNodeId);

				if (nodeInterval.intersection(treeNodeInterval) != null) {
					if (!Arrays.equals(childTreeNode.getHash(), remoteTreeNode.getChildHash(i)))
						synchronizeNode(localDHTNode, childTreeNode, remoteNodeAddress);
				}
			}
		}
	}

	private void compareNodes(final ODHTNodeLocal localDHTNode,
														ODetachedMerkleTreeNode localTreeNode, ODetachedMerkleTreeNode remoteNode, ONodeAddress remoteNodeAddress) {
		if (Arrays.equals(remoteNode.getHash(), localTreeNode.getHash()))
			return;

		fetchRemoteRecordsToLocalReplica(localDHTNode, localTreeNode, remoteNode, remoteNodeAddress);
		sendLocalRecordsToRemoteReplica(localDHTNode, localTreeNode, remoteNode, remoteNodeAddress);
	}

	private void sendLocalRecordsToRemoteReplica(final ODHTNodeLocal localDHTNode,
																							 ODetachedMerkleTreeNode localTreeNode,
																							 ODetachedMerkleTreeNode remoteNode,
																							 ONodeAddress remoteNodeAddress) {
		final ONodeAddress localPredecessor = localDHTNode.getPredecessor();

		if (localPredecessor == null)
			throw new ONodeSynchronizationFailedException("Node predecessor is absent.");

		final ODHTNode remoteDHTNode = nodeLookup.findById(remoteNodeAddress);
		if (remoteDHTNode == null)
			throw new ONodeSynchronizationFailedException("Remote node with id " + remoteNodeAddress + " is offline.");

		final ODHTRingInterval dhtNodeInterval = new ODHTRingInterval(localPredecessor.getNodeId().add(ONodeId.ONE),
				localDHTNode.getNodeAddress().getNodeId());

		final ODHTRingInterval treeNodeInterval = new ODHTRingInterval(localTreeNode.getStartId(), localTreeNode.getEndId());

		final ODHTRingInterval recordsInterval = dhtNodeInterval.intersection(treeNodeInterval);
		if (recordsInterval == null)
			return;

		ONodeId startId = recordsInterval.getStart();
		ONodeId endId = recordsInterval.getEnd();

		final ArrayList<Record> recordsToReplicate = new ArrayList<Record>();
		if (localTreeNode.isLeaf()) {
			RecordMetadata[] nodeMetadatas =
							localDHTNode.getRecordsForIntervalFromNode(new ORecordId(1, new OClusterPositionNodeId(startId)),
							new ORecordId(1, new OClusterPositionNodeId(endId)));

			while (nodeMetadatas.length > 0) {
				final ORID[] missedIds = remoteDHTNode.findMissedRecords(nodeMetadatas);
				for (ORID missedId : missedIds) {
					final Record record = localDHTNode.readRecordLocal(missedId);
					if (record != null)
						recordsToReplicate.add(record);

					if (recordsToReplicate.size() >= 64) {
						sendRecords(recordsToReplicate, remoteNodeAddress);
						recordsToReplicate.clear();
					}

				}

				startId = ((OClusterPositionNodeId) nodeMetadatas[nodeMetadatas.length - 1].getId().getClusterPosition()).getNodeId().add(
						ONodeId.ONE);

				if (recordsInterval.insideInterval(startId))
					nodeMetadatas = localDHTNode.getRecordsForIntervalFromNode(new ORecordId(1, new OClusterPositionNodeId(startId)),
									new ORecordId(1, new OClusterPositionNodeId(endId)));
			}

			if (!recordsToReplicate.isEmpty())
				sendRecords(recordsToReplicate, remoteNodeAddress);
		} else if (remoteNode.isLeaf()) {
			final Set<RecordMetadata> merkleTreeMetadataSet = new HashSet<RecordMetadata>();

			RecordMetadata[] recordMetadatas =
							localDHTNode.getRecordsForIntervalFromNode(new ORecordId(1, new OClusterPositionNodeId(startId)),
							new ORecordId(1, new OClusterPositionNodeId(endId)));

			for (int i = 0; i < remoteNode.getRecordsCount(); i++)
				merkleTreeMetadataSet.add(remoteNode.getRecordMetadata(i));

			while (recordMetadatas.length > 0) {
				for (RecordMetadata recordMetadata : recordMetadatas) {
					if (!merkleTreeMetadataSet.contains(recordMetadata)) {
						final Record record = localDHTNode.readRecordLocal(recordMetadata.getId());
						if (record != null)
							recordsToReplicate.add(record);
					}

					if (recordsToReplicate.size() >= 64) {
						sendRecords(recordsToReplicate, remoteNodeAddress);
						recordsToReplicate.clear();
					}
				}
				startId = ((OClusterPositionNodeId) recordMetadatas[recordMetadatas.length - 1].getId().getClusterPosition()).getNodeId().add(
						ONodeId.ONE);

				if (recordsInterval.insideInterval(startId))
					recordMetadatas = localDHTNode.getRecordsForIntervalFromNode(new ORecordId(1, new OClusterPositionNodeId(startId)), new ORecordId(1,
									new OClusterPositionNodeId(endId)));
			}

			if (!recordsToReplicate.isEmpty())
				sendRecords(recordsToReplicate, remoteNodeAddress);

		}
	}

	private void fetchRemoteRecordsToLocalReplica(final ODHTNodeLocal localDHTNode,
																								ODetachedMerkleTreeNode localTreeNode,
																								ODetachedMerkleTreeNode remoteNode,
																								ONodeAddress remoteNodeAddress) {
		final ONodeAddress localPredecessor = localDHTNode.getPredecessor();

		if (localPredecessor == null)
			throw new ONodeSynchronizationFailedException("Node predecessor is absent.");

		final ODHTNode remoteDHTNode = nodeLookup.findById(remoteNodeAddress);
		if (remoteDHTNode == null)
			throw new ONodeSynchronizationFailedException("Remote node with id " + remoteNodeAddress + " is offline.");

    final ODHTRingInterval dhtNodeInterval = new ODHTRingInterval(localPredecessor.getNodeId().add(ONodeId.ONE),
				localDHTNode.getNodeAddress().getNodeId());

		final ArrayList<ORID> recordsToFetch = new ArrayList<ORID>();

		if (remoteNode.isLeaf()) {
			for (int i = 0; i < remoteNode.getRecordsCount(); i++) {
				final RecordMetadata recordMetadata = remoteNode.getRecordMetadata(i);

				if (dhtNodeInterval.insideInterval(((OClusterPositionNodeId) recordMetadata.getId().getClusterPosition()).getNodeId())) {
					final Record dbRecord = localDHTNode.readRecordLocal(recordMetadata.getId());
					if (dbRecord == null || dbRecord.getVersion().compareTo(recordMetadata.getVersion()) < 0)
						recordsToFetch.add(recordMetadata.getId());
				}

				if (recordsToFetch.size() >= 64) {
					fetchRecords(localDHTNode, recordsToFetch, remoteNodeAddress);
					recordsToFetch.clear();
				}
			}

			if (!recordsToFetch.isEmpty())
				fetchRecords(localDHTNode, recordsToFetch, remoteNodeAddress);

		} else if (localTreeNode.isLeaf()) {
			final ODHTRingInterval treeNodeInterval = new ODHTRingInterval(localTreeNode.getStartId(), localTreeNode.getEndId());

			final ODHTRingInterval recordsInterval = dhtNodeInterval.intersection(treeNodeInterval);
			if (recordsInterval == null)
				return;

			ONodeId startId = recordsInterval.getStart();
			ONodeId endId = recordsInterval.getEnd();


			RecordMetadata[] nodeMetadatas = remoteDHTNode.getRecordsForIntervalFromNode(new ORecordId(1, new OClusterPositionNodeId(
					startId)), new ORecordId(1, new OClusterPositionNodeId(endId)));

			while (nodeMetadatas.length > 0) {
				for (RecordMetadata nodeMetadata : nodeMetadatas) {
					final Record dbRecord = localDHTNode.readRecordLocal(nodeMetadata.getId());

					if (dbRecord == null || dbRecord.getVersion().compareTo(nodeMetadata.getVersion()) < 0)
						recordsToFetch.add(nodeMetadata.getId());

					if (recordsToFetch.size() >= 64) {
						fetchRecords(localDHTNode, recordsToFetch, remoteNodeAddress);
						recordsToFetch.clear();
					}
				}

				startId = ((OClusterPositionNodeId) nodeMetadatas[nodeMetadatas.length - 1].getId().getClusterPosition()).getNodeId().add(
						ONodeId.ONE);

				if (recordsInterval.insideInterval(startId))
					nodeMetadatas = remoteDHTNode.getRecordsForIntervalFromNode(new ORecordId(1, new OClusterPositionNodeId(startId)),
							new ORecordId(1, new OClusterPositionNodeId(endId)));
			}

			if (!recordsToFetch.isEmpty())
				fetchRecords(localDHTNode, recordsToFetch, remoteNodeAddress);
		}
	}

	private void fetchRecords(final ODHTNodeLocal localDHTNode,
														List<ORID> missedRecords, ONodeAddress remoteNodeId) {
		final ODHTNode remoteNode = nodeLookup.findById(remoteNodeId);
		if (remoteNode == null)
			throw new ONodeSynchronizationFailedException("Node with id " + remoteNodeId + " is absent in ring.");

		final ORID[] missedRecordsArray = new ORID[missedRecords.size()];
		for (int i = 0; i < missedRecordsArray.length; i++)
			missedRecordsArray[i] = missedRecords.get(i);

		final Record[] replicas = remoteNode.getRecordsFromNode(missedRecordsArray);

		for (Record replica : replicas) {
			localDHTNode.updateReplica(replica, false);
		}
	}

	private void sendRecords(List<Record> missedRecords, ONodeAddress remoteNodeId) {
		final ODHTNode remoteNode = nodeLookup.findById(remoteNodeId);
		if (remoteNode == null)
			throw new ONodeSynchronizationFailedException("Node with id " + remoteNodeId + " is absent in ring.");

		Record[] missedRecordsArray = new Record[missedRecords.size()];
		missedRecordsArray = missedRecords.toArray(missedRecordsArray);

		remoteNode.updateReplicas(missedRecordsArray, false);
	}
}