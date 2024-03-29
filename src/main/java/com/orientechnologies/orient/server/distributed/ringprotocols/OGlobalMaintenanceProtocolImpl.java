package com.orientechnologies.orient.server.distributed.ringprotocols;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.id.OClusterPositionNodeId;
import com.orientechnologies.orient.core.id.ONodeId;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.server.distributed.ODHTNode;
import com.orientechnologies.orient.server.distributed.ODHTNodeLocal;
import com.orientechnologies.orient.server.distributed.ODHTNodeLookup;
import com.orientechnologies.orient.server.distributed.ONodeAddress;
import com.orientechnologies.orient.server.distributed.ONodeOfflineException;
import com.orientechnologies.orient.server.distributed.ORecordMetadata;

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
  public ONodeId reallocateWrongPlacedReplicas(String storageName, int clusterId, final ODHTNodeLocal nodeLocal, ONodeId idToTest,
      int replicaCount, int syncReplicaCount) {
    final ONodeAddress localNodeAddress = nodeLocal.getNodeAddress();

    final ODHTNode.NodeState nodeState = nodeLocal.state();

    if (nodeState == null || !nodeState.equals(ODHTNode.NodeState.PRODUCTION))
      return localNodeAddress.getNodeId();

    final ORID nextRecordId = nextInDB(storageName, nodeLocal, new ORecordId(clusterId, new OClusterPositionNodeId(idToTest)));
    if (nextRecordId == null)
      return localNodeAddress.getNodeId();

    final ONodeId nextId = ((OClusterPositionNodeId) nextRecordId.getClusterPosition()).getNodeId();

    final ONodeAddress successor = nodeLocal.findSuccessor(nextId);

    if (localNodeAddress.equals(successor))
      return localNodeAddress.getNodeId();

    final ODHTNode successorNode = nodeLookup.findById(successor);
    if (successorNode == null)
      return localNodeAddress.getNodeId();

    final Set<ONodeAddress> replicaHolderAddresses = new HashSet<ONodeAddress>();
    final Set<ONodeAddress>[] replicaHolders = replicaDistributionStrategy.chooseReplicas(successorNode.getSuccessors(),
        replicaCount, syncReplicaCount);

    replicaHolderAddresses.addAll(replicaHolders[0]);
    replicaHolderAddresses.addAll(replicaHolders[1]);

    for (ONodeAddress s : replicaHolderAddresses) {
      if (s.equals(localNodeAddress))
        return localNodeAddress.getNodeId();
    }

    final List<ONodeAddress> nodesToReplicate = new ArrayList<ONodeAddress>();
    nodesToReplicate.add(successor);
    nodesToReplicate.addAll(replicaHolderAddresses);

    final Iterator<ORecordMetadata> iterator = nodeLocal.getLocalRingIterator(storageName, new ORecordId(clusterId,
						new OClusterPositionNodeId(idToTest)), new ORecordId(clusterId, new OClusterPositionNodeId(successor.getNodeId())));

    final List<ORecordMetadata> recordMetadatas = new ArrayList<ORecordMetadata>(64);

    while (iterator.hasNext()) {
      final ORecordMetadata recordMetadata = iterator.next();

      recordMetadatas.add(recordMetadata);

      if (recordMetadatas.size() >= 64)
        cleanOutForeignRecords(storageName, nodeLocal, recordMetadatas, nodesToReplicate);
    }

    if (!recordMetadatas.isEmpty())
      cleanOutForeignRecords(storageName, nodeLocal, recordMetadatas, nodesToReplicate);

    return successor.getNodeId();
  }

  private void cleanOutForeignRecords(String storageName, final ODHTNodeLocal nodeLocal, List<ORecordMetadata> recordMetadatas,
      List<ONodeAddress> nodesToReplicate) {
    ORecordMetadata[] metadatas = new ORecordMetadata[recordMetadatas.size()];
    metadatas = recordMetadatas.toArray(metadatas);

    for (ONodeAddress replicaHolderAddress : nodesToReplicate) {
      final ODHTNode node = nodeLookup.findById(replicaHolderAddress);

      if (node == null)
        continue;

      try {
        final ORID[] missingIds = node.findMissedRecords(storageName, metadatas);

        for (ORID missingId : missingIds) {
          final ORecordInternal<?> replica = nodeLocal.readRecordLocal(storageName, missingId);
          if (replica != null)
            node.updateReplica(storageName, replica, false);
        }

      } catch (ONodeOfflineException noe) {
        logger.error("Node with id {} is absent. Continue replication with other node.", replicaHolderAddress);
      }
    }

    for (ORecordMetadata recordMetadata : metadatas) {
      try {
        nodeLocal.cleanOutRecord(storageName, recordMetadata.getRid(), recordMetadata.getVersion());
      } catch (OConcurrentModificationException e) {
        logger.error("Record with id {} and version {} is out of date and can not be cleaned out", recordMetadata.getRid(),
            recordMetadata.getVersion());
      } catch (ORecordNotFoundException e) {
        logger.error("Record with id {} is absent and can not be cleaned out", recordMetadata.getRid());
      }
    }

    recordMetadatas.clear();
  }

  private ORID nextInDB(String storageName, final ODHTNodeLocal nodeLocal, ORID id) {
    final Iterator<ORecordMetadata> ringIterator = nodeLocal.getLocalRingIterator(storageName, id.nextRid(), id);

    if (ringIterator.hasNext()) {
      final ORecordMetadata result = ringIterator.next();

      if (result.getRid().equals(id))
        return null;

      return result.getRid();
    }

    return null;
  }
}
