package com.orientechnologies.orient.server.distributed.ringprotocols.localcomparators;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.orientechnologies.orient.core.id.OClusterPositionNodeId;
import com.orientechnologies.orient.core.id.ONodeId;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.server.distributed.ODHTNode;
import com.orientechnologies.orient.server.distributed.ODHTNodeLocal;
import com.orientechnologies.orient.server.distributed.ODHTNodeLookup;
import com.orientechnologies.orient.server.distributed.ODHTRingInterval;
import com.orientechnologies.orient.server.distributed.ONodeAddress;
import com.orientechnologies.orient.server.distributed.ORecordMetadata;
import com.orientechnologies.orient.server.distributed.Record;
import com.orientechnologies.orient.server.distributed.merkletree.ODetachedMerkleTreeNode;
import com.orientechnologies.orient.server.distributed.ringprotocols.OLocalMaintenanceProtocolNodeComparator;
import com.orientechnologies.orient.server.distributed.ringprotocols.OLocalProtocolException;

/**
 * @author Andrey Lomakin
 * @since 30.11.12
 */
public final class ORemoteWithLocalNodeComparator implements OLocalMaintenanceProtocolNodeComparator {
	private final ODHTNodeLookup nodeLookup;

	public ORemoteWithLocalNodeComparator(ODHTNodeLookup nodeLookup) {
		this.nodeLookup = nodeLookup;
	}

	@Override
  public void compareNodes(ODHTNodeLocal localDHTNode, ODetachedMerkleTreeNode localTreeNode, ODetachedMerkleTreeNode remoteNode,
      ONodeAddress remoteNodeAddress) {
    if (Arrays.equals(remoteNode.getHash(), localTreeNode.getHash()))
      return;

    final ONodeAddress localPredecessor = localDHTNode.getPredecessor();

    if (localPredecessor == null)
      throw new OLocalProtocolException("Node predecessor is absent.");

    final ODHTNode remoteDHTNode = nodeLookup.findById(remoteNodeAddress);
    if (remoteDHTNode == null)
      throw new OLocalProtocolException("Remote node with id " + remoteNodeAddress + " is offline.");

    final ODHTRingInterval dhtNodeInterval = new ODHTRingInterval(localPredecessor.getNodeId().add(ONodeId.ONE), localDHTNode
        .getNodeAddress().getNodeId());

    if (remoteNode.isLeaf())
			remoteNodeIsLeaf(localDHTNode, remoteNode, remoteNodeAddress, dhtNodeInterval);
    else if (localTreeNode.isLeaf())
			localNodeIsLeaf(localDHTNode, localTreeNode, remoteNodeAddress, remoteDHTNode, dhtNodeInterval);
  }

	private void localNodeIsLeaf(ODHTNodeLocal localDHTNode, ODetachedMerkleTreeNode localTreeNode,
															 ONodeAddress remoteNodeAddress, ODHTNode remoteDHTNode, ODHTRingInterval dhtNodeInterval) {
		final ArrayList<ORID> recordsToFetch = new ArrayList<ORID>();

		final ODHTRingInterval treeNodeInterval = new ODHTRingInterval(localTreeNode.getStartId(), localTreeNode.getEndId());

		final ODHTRingInterval recordsInterval = dhtNodeInterval.intersection(treeNodeInterval);
		if (recordsInterval == null)
			return;

		ONodeId startId = recordsInterval.getStart();
		ONodeId endId = recordsInterval.getEnd();

		ORecordMetadata[] nodeMetadatas = remoteDHTNode.getRecordsForIntervalFromNode(new ORecordId(1, new OClusterPositionNodeId(
				startId)), new ORecordId(1, new OClusterPositionNodeId(endId)));

		while (nodeMetadatas.length > 0) {
			for (ORecordMetadata nodeMetadata : nodeMetadatas) {
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

	private void remoteNodeIsLeaf(ODHTNodeLocal localDHTNode, ODetachedMerkleTreeNode remoteNode,
																ONodeAddress remoteNodeAddress, ODHTRingInterval dhtNodeInterval) {
		final ArrayList<ORID> recordsToFetch = new ArrayList<ORID>();

		for (int i = 0; i < remoteNode.getRecordsCount(); i++) {
			final ORecordMetadata recordMetadata = remoteNode.getRecordMetadata(i);

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
	}

	private void fetchRecords(final ODHTNodeLocal localDHTNode, List<ORID> missedRecords, ONodeAddress remoteNodeId) {
    final ODHTNode remoteNode = nodeLookup.findById(remoteNodeId);
    if (remoteNode == null)
      throw new OLocalProtocolException("Node with id " + remoteNodeId + " is absent in ring.");

    final ORID[] missedRecordsArray = new ORID[missedRecords.size()];
    for (int i = 0; i < missedRecordsArray.length; i++)
      missedRecordsArray[i] = missedRecords.get(i);

    final Record[] replicas = remoteNode.getRecordsFromNode(missedRecordsArray);

    for (Record replica : replicas) {
      localDHTNode.updateReplica(replica, false);
    }
  }

}
