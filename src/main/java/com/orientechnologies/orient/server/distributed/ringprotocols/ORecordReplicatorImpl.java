package com.orientechnologies.orient.server.distributed.ringprotocols;

import java.util.Set;

import com.orientechnologies.orient.core.record.ORecordInternal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.server.distributed.ODHTNode;
import com.orientechnologies.orient.server.distributed.ODHTNodeLookup;
import com.orientechnologies.orient.server.distributed.ONodeAddress;
import com.orientechnologies.orient.server.distributed.Record;

/**
 * @author Andrey Lomakin
 * @since 22.11.12
 */
public final class ORecordReplicatorImpl implements ORecordReplicator {
  private static final Logger                LOGGER = LoggerFactory.getLogger(ORecordReplicatorImpl.class);

  private final ODHTNodeLookup               nodeLookup;
  private final OReplicaDistributionStrategy replicaDistributionStrategy;

  public ORecordReplicatorImpl(ODHTNodeLookup nodeLookup, OReplicaDistributionStrategy replicaDistributionStrategy) {
    this.nodeLookup = nodeLookup;
    this.replicaDistributionStrategy = replicaDistributionStrategy;
  }

  @Override
  public void replicateRecord(ODHTNode node, String storageName, ORID recordId, int replicaCount, int syncReplicaCount) {
    if (replicaCount < 1)
      return;

    ONodeAddress[] successors = node.getSuccessors();
    if (successors == null || successors.length == 0)
      return;

    final Set<ONodeAddress>[] replicas = replicaDistributionStrategy.chooseReplicas(successors, replicaCount, syncReplicaCount);
    final Set<ONodeAddress> syncReplicas = replicas[0];
    final Set<ONodeAddress> asyncReplicas = replicas[1];

    final ORecordInternal<?> record = node.getRecordFromNode(storageName, recordId);

    if (record == null)
      return;

    for (ONodeAddress syncReplicaHolderAddress : syncReplicas) {
      final ODHTNode replicaHolderNode = nodeLookup.findById(syncReplicaHolderAddress);
      if (replicaHolderNode == null) {
        LOGGER.error("Replica holder with id " + syncReplicaHolderAddress + " is absent.");
        continue;
      }

      replicateRecord(replicaHolderNode, storageName, record, false);
    }

    for (ONodeAddress asyncReplicaHolderAddress : asyncReplicas) {
      final ODHTNode replicaHolderNode = nodeLookup.findById(asyncReplicaHolderAddress);
      if (replicaHolderNode == null) {
        LOGGER.error("Replica holder with id " + asyncReplicaHolderAddress + " is absent.");
        continue;
      }

      replicateRecord(replicaHolderNode, storageName, record, true);
    }
  }

  private void replicateRecord(ODHTNode replicaHolderNode, String storageName, ORecordInternal<?> record, boolean async) {
    try {
      replicaHolderNode.updateReplica(storageName, record, async);
    } catch (Exception e) {
      LOGGER
          .error("Exception during replication of record " +
									record.getIdentity() + " to node " + replicaHolderNode.getNodeAddress(), e);
    }
  }
}
