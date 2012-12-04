package com.orientechnologies.orient.server.distributed.ringprotocols.localcomparators;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
public final class OLocalWithRemoteNodeComparator implements OLocalMaintenanceProtocolNodeComparator {
  private final ODHTNodeLookup nodeLookup;

  public OLocalWithRemoteNodeComparator(ODHTNodeLookup nodeLookup) {
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

    final ODHTRingInterval treeNodeInterval = new ODHTRingInterval(localTreeNode.getStartId(), localTreeNode.getEndId());

    final ODHTRingInterval recordsInterval = dhtNodeInterval.intersection(treeNodeInterval);
    if (recordsInterval == null)
      return;

    if (localTreeNode.isLeaf())
      localNodeIsLeaf(localDHTNode, remoteNodeAddress, remoteDHTNode, recordsInterval);
    else if (remoteNode.isLeaf())
      remoteNodeIsLeaf(localDHTNode, remoteNode, remoteNodeAddress, recordsInterval);

  }

  private void remoteNodeIsLeaf(ODHTNodeLocal localDHTNode, ODetachedMerkleTreeNode remoteNode,
																ONodeAddress remoteNodeAddress,
     														ODHTRingInterval recordsInterval) {
    ONodeId startId = recordsInterval.getStart();
    ONodeId endId = recordsInterval.getEnd();

    final ArrayList<Record> recordsToReplicate = new ArrayList<Record>();

    final Set<ORecordMetadata> merkleTreeMetadataSet = new HashSet<ORecordMetadata>();

    ORecordMetadata[] recordMetadatas = localDHTNode.getRecordsForIntervalFromNode(new ORecordId(1, new OClusterPositionNodeId(
        startId)), new ORecordId(1, new OClusterPositionNodeId(endId)));

    for (int i = 0; i < remoteNode.getRecordsCount(); i++)
      merkleTreeMetadataSet.add(remoteNode.getRecordMetadata(i));

    while (recordMetadatas.length > 0) {
      for (ORecordMetadata recordMetadata : recordMetadatas) {
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
      startId = ((OClusterPositionNodeId) recordMetadatas[recordMetadatas.length - 1].getId().getClusterPosition()).getNodeId()
          .add(ONodeId.ONE);

      if (recordsInterval.insideInterval(startId))
        recordMetadatas = localDHTNode.getRecordsForIntervalFromNode(new ORecordId(1, new OClusterPositionNodeId(startId)),
            new ORecordId(1, new OClusterPositionNodeId(endId)));
    }

    if (!recordsToReplicate.isEmpty())
      sendRecords(recordsToReplicate, remoteNodeAddress);
  }

  private void localNodeIsLeaf(ODHTNodeLocal localDHTNode, ONodeAddress remoteNodeAddress,
															 ODHTNode remoteDHTNode,
      												 ODHTRingInterval recordsInterval) {
    ONodeId startId = recordsInterval.getStart();
    ONodeId endId = recordsInterval.getEnd();

    final ArrayList<Record> recordsToReplicate = new ArrayList<Record>();

    ORecordMetadata[] nodeMetadatas = localDHTNode.getRecordsForIntervalFromNode(new ORecordId(1, new OClusterPositionNodeId(
        startId)), new ORecordId(1, new OClusterPositionNodeId(endId)));

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
  }

  private void sendRecords(List<Record> missedRecords, ONodeAddress remoteNodeId) {
    final ODHTNode remoteNode = nodeLookup.findById(remoteNodeId);
    if (remoteNode == null)
      throw new OLocalProtocolException("Node with id " + remoteNodeId + " is absent in ring.");

    Record[] missedRecordsArray = new Record[missedRecords.size()];
    missedRecordsArray = missedRecords.toArray(missedRecordsArray);

    remoteNode.updateReplicas(missedRecordsArray, false);
  }
}
