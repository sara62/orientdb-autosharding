package com.orientechnologies.orient.server.distributed.ringprotocols;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.id.OClusterPositionNodeId;
import com.orientechnologies.orient.core.id.ONodeId;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.server.distributed.ODHTNode;
import com.orientechnologies.orient.server.distributed.ODHTNodeLocal;
import com.orientechnologies.orient.server.distributed.ODHTNodeLookup;
import com.orientechnologies.orient.server.distributed.ONodeAddress;
import com.orientechnologies.orient.server.distributed.ONodeOfflineException;
import com.orientechnologies.orient.server.distributed.Record;
import com.orientechnologies.orient.server.distributed.RecordMetadata;

/**
 * @author Andrey Lomakin
 * @since 23.11.12
 */
public final class OGlobalMaintenanceProtocolImpl implements OGlobalMaintenanceProtocol {
  private final Logger                       logger = LoggerFactory.getLogger(OGlobalMaintenanceProtocolImpl.class);

  private final ODHTNodeLookup               nodeLookup;
  private final OReplicaDistributionStrategy replicaDistributionStrategy;

  public OGlobalMaintenanceProtocolImpl(ODHTNodeLookup nodeLookup, OReplicaDistributionStrategy replicaDistributionStrategy) {
    this.nodeLookup = nodeLookup;
    this.replicaDistributionStrategy = replicaDistributionStrategy;
  }

  @Override
  public ONodeId reallocateWrongPlacedReplicas(final ODHTNodeLocal nodeLocal, ONodeId idToTest,
																							 int replicaCount, int syncReplicaCount) {
    if (nodeLocal.state() == null || !nodeLocal.state().equals(ODHTNode.NodeState.PRODUCTION))
      return nodeLocal.getNodeAddress().getNodeId();

    final ORecordId nextRecordId = nextInDB(nodeLocal, new ORecordId(1, new OClusterPositionNodeId(idToTest)));
    if (nextRecordId == null)
      return idToTest;

    ONodeId nextId = ((OClusterPositionNodeId) nextRecordId.clusterPosition).getNodeId();

    ONodeAddress successor = nodeLocal.findSuccessor(nextId);

    if (nodeLocal.getNodeAddress().equals(successor)) {
      return idToTest;
    }

    final ODHTNode successorNode = nodeLookup.findById(successor);
    if (successorNode == null)
      return nodeLocal.getNodeAddress().getNodeId();

    final Set<ONodeAddress> replicaHolderAddresses = new HashSet<ONodeAddress>();
    final Set<ONodeAddress>[] replicaHolders = replicaDistributionStrategy.chooseReplicas(successorNode.getSuccessors(),
						replicaCount,
						syncReplicaCount);

    replicaHolderAddresses.addAll(replicaHolders[0]);
    replicaHolderAddresses.addAll(replicaHolders[1]);

    for (ONodeAddress s : replicaHolderAddresses) {
      if (s.equals(nodeLocal.getNodeAddress()))
        return nodeLocal.getNodeAddress().getNodeId();

    }

    List<ONodeAddress> nodesToReplicate = new ArrayList<ONodeAddress>();
    nodesToReplicate.add(successor);
    nodesToReplicate.addAll(replicaHolderAddresses);

    final Iterator<RecordMetadata> iterator = nodeLocal
        .getLocalRingIterator(new ORecordId(1, new OClusterPositionNodeId(idToTest)), new ORecordId(1, new OClusterPositionNodeId(
            successor.getNodeId())));

    final List<RecordMetadata> recordMetadatas = new ArrayList<RecordMetadata>(64);

    while (iterator.hasNext()) {
      final RecordMetadata recordMetadata = iterator.next();

      recordMetadatas.add(recordMetadata);

      if (recordMetadatas.size() >= 64)
        cleanOutForeignRecords(nodeLocal, recordMetadatas, nodesToReplicate);
    }

    if (!recordMetadatas.isEmpty())
      cleanOutForeignRecords(nodeLocal, recordMetadatas, nodesToReplicate);

    return successor.getNodeId();
  }

  private void cleanOutForeignRecords(final ODHTNodeLocal nodeLocal, List<RecordMetadata> recordMetadatas,
      List<ONodeAddress> nodesToReplicate) {
    RecordMetadata[] metadatas = new RecordMetadata[recordMetadatas.size()];
    metadatas = recordMetadatas.toArray(metadatas);

    for (ONodeAddress replicaHolderAddress : nodesToReplicate) {
      final ODHTNode node = nodeLookup.findById(replicaHolderAddress);

      if (node == null)
        continue;

      try {
        final ORecordId[] missingIds = node.findMissedRecords(metadatas);

        for (ORecordId missingId : missingIds) {
          final Record replica = nodeLocal.readRecordLocal(missingId);
          if (replica != null)
            node.updateReplica(replica, false);
        }

      } catch (ONodeOfflineException noe) {
        logger.error("Node with id {} is absent. Continue replication with other node.", replicaHolderAddress);
      }
    }

    for (RecordMetadata recordMetadata : metadatas) {
      try {
        nodeLocal.cleanOutData(recordMetadata.getId(), recordMetadata.getVersion());
      } catch (OConcurrentModificationException e) {
        logger.error("Record with id {} and version {} is out of date and can not be cleaned out", recordMetadata.getId(),
            recordMetadata.getVersion());
      }
    }

    recordMetadatas.clear();
  }

  private ORecordId nextInDB(final ODHTNodeLocal nodeLocal, ORecordId id) {
    ORecordId result = nodeLocal.getHigherLocalId(id);

    if (result != null)
      return result;

    if (id.compareTo(new ORecordId(1, new OClusterPositionNodeId(ONodeId.ZERO))) > 0)
      result = nodeLocal.getCeilingLocalId(new ORecordId(1, new OClusterPositionNodeId(ONodeId.ZERO)));

    if (result != null && result != id)
      return result;

    return null;
  }
}
