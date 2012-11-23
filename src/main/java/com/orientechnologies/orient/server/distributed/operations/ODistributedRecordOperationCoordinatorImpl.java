package com.orientechnologies.orient.server.distributed.operations;

import com.orientechnologies.orient.server.distributed.ODHTNode;
import com.orientechnologies.orient.server.distributed.ODHTNodeLookup;
import com.orientechnologies.orient.server.distributed.ONodeAddress;
import com.orientechnologies.orient.server.distributed.ONodeOfflineException;
import com.orientechnologies.orient.server.distributed.util.OWaitTillNodeJoin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.orientechnologies.orient.core.id.OClusterPositionNodeId;
import com.orientechnologies.orient.core.id.ONodeId;
import com.orientechnologies.orient.core.id.ORecordId;

/**
 * @author Andrey Lomakin
 * @since 21.11.12
 */
public final class ODistributedRecordOperationCoordinatorImpl implements ODistributedRecordOperationCoordinator {
  private static final int     MAX_RETRIES = 10;

  private static final Logger  LOGGER      = LoggerFactory.getLogger(ODistributedRecordOperationCoordinatorImpl.class);

  private final ODHTNodeLookup nodeLookup;

  public ODistributedRecordOperationCoordinatorImpl(ODHTNodeLookup nodeLookup) {
    this.nodeLookup = nodeLookup;
  }

  public <T> T executeRecordOperation(ODHTNode node, ODistributedRecordOperation<T> recordOperation) {
		OWaitTillNodeJoin.waitTillNodeJoin(node);

    int retryCount = 0;

    while (true) {
      retryCount++;

      if (recordOperation.getClusterPosition() == null) {
        recordOperation.setRecordId(new ORecordId(recordOperation.getClusterId(), new OClusterPositionNodeId(ONodeId
            .generateUniqueId())));
      }

      final ONodeId clusterPosition = ((OClusterPositionNodeId) recordOperation.getClusterPosition()).getNodeId();

      final ONodeAddress successorAddress = node.findSuccessor(clusterPosition);

      LOGGER.debug("Successor for cluster position {} is {}", clusterPosition, successorAddress);
      retryCount++;

      if (successorAddress.equals(node.getNodeAddress())) {
        return recordOperation.execute(node);
      } else {
        final ORemoteNodeCallResult<T> result = executeOperationOnRemoteNode(recordOperation, retryCount, successorAddress);
        if (result.isRepeat()) {
          LOGGER.error("Successor {} for cluster position {} is offline, retry.", successorAddress, clusterPosition);
          node.requestStabilization();
          continue;
        }

        return result.getData();
      }
    }
  }

  private <T> ORemoteNodeCallResult<T> executeOperationOnRemoteNode(ODistributedRecordOperation<T> recordOperation, int retryCount,
      ONodeAddress remoteNodeAddress) {
    final ODHTNode node = nodeLookup.findById(remoteNodeAddress);

    if (node == null) {
      if (retryCount < MAX_RETRIES) {
        LOGGER.debug("Node {} is offline, retry {}-d time.", remoteNodeAddress, retryCount);
        return new ORemoteNodeCallResult<T>(true, null);
      } else {
        LOGGER.error("Node {} is offline, retry limit is reached.", remoteNodeAddress);
        throw new ONodeOfflineException("Node " + remoteNodeAddress + " is offline, retry limit is reached.", null,
            remoteNodeAddress.getNodeId());
      }
    }

    try {
      return new ORemoteNodeCallResult<T>(false, recordOperation.execute(node));
    } catch (ONodeOfflineException ooe) {
      if (retryCount < MAX_RETRIES) {
        LOGGER.debug("Node {} is offline, retry {}-d time.", remoteNodeAddress, retryCount);

        return new ORemoteNodeCallResult<T>(true, null);
      } else {
        LOGGER.error("Node " + remoteNodeAddress + " is offline, retry limit is reached.");
        throw new ONodeOfflineException("Node " + remoteNodeAddress + " is offline, retry limit is reached.", null,
            remoteNodeAddress.getNodeId());
      }
    }
  }

}
