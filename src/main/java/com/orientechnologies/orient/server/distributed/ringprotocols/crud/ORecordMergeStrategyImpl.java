package com.orientechnologies.orient.server.distributed.ringprotocols.crud;

import java.util.HashSet;
import java.util.Set;

import com.orientechnologies.orient.server.distributed.ODHTNodeLocal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.server.distributed.ODHTNode;
import com.orientechnologies.orient.server.distributed.ODHTNodeLookup;
import com.orientechnologies.orient.server.distributed.ONodeAddress;
import com.orientechnologies.orient.server.distributed.Record;
import com.orientechnologies.orient.server.distributed.RecordMetadata;

/**
 * @author Andrey Lomakin
 * @since 22.11.12
 */
public class ORecordMergeStrategyImpl implements ORecordMergeStrategy {
  private static final Logger  LOGGER = LoggerFactory.getLogger(ORecordMergeStrategyImpl.class);

  private final ODHTNodeLookup nodeLookup;

  public ORecordMergeStrategyImpl(ODHTNodeLookup nodeLookup) {
    this.nodeLookup = nodeLookup;
  }

  @Override
  public void mergeReplicaVersions(ODHTNodeLocal localNode, ORecordId recordId, Set<ONodeAddress> replicaHolders) {
    final Set<ONodeAddress> replicaHoldersToUpdate = new HashSet<ONodeAddress>();

    try {
      ODHTNode primaryHolder = localNode;
      RecordMetadata primaryMetadata = primaryHolder.getRecordMetadataFromNode(recordId);

      for (ONodeAddress holderId : replicaHolders) {
        final ODHTNode holderNode = nodeLookup.findById(holderId);
        if (holderNode == null)
          continue;

        try {
          final RecordMetadata nodeMetadata = holderNode.getRecordMetadataFromNode(recordId);
          if (primaryMetadata == null) {
            if (nodeMetadata != null) {
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
              } else if (cp > 0) {
                replicaHoldersToUpdate.add(holderNode.getNodeAddress());
              }
            } else {
              replicaHoldersToUpdate.add(holderNode.getNodeAddress());
            }
          }
        } catch (Exception e) {
          // ignore
          LOGGER.error("Exception during synchronization of record " + recordId + " for node " + holderId, e);
        }
      }

      if (!replicaHoldersToUpdate.isEmpty()) {
        final Record result;

        if (localNode.getNodeAddress().equals(primaryHolder.getNodeAddress()))
          result = localNode.readData(recordId);
        else
          result = primaryHolder.getRecordFromNode(recordId);

        if (result == null)
          return;

        for (ONodeAddress replicaHolderAddress : replicaHoldersToUpdate) {
          final ODHTNode replicaHolder = nodeLookup.findById(replicaHolderAddress);
          if (replicaHolder == null)
            continue;
          try {
            replicaHolder.updateReplica(result, false);
          } catch (Exception e) {
            LOGGER.error(
                "Exception during replication of record with id " + recordId + " for node " + replicaHolder.getNodeAddress(), e);
          }
        }
      }
    } catch (Exception e) {
			LOGGER.error(
							"Exception during merger of replicas of record with id " + recordId + " for node " + localNode.getNodeAddress(), e);
		}
  }
}
