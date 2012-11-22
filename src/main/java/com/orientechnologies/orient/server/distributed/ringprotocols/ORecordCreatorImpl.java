package com.orientechnologies.orient.server.distributed.ringprotocols;

import com.orientechnologies.orient.server.distributed.ODHTNode;
import com.orientechnologies.orient.server.distributed.ODHTNodeLookup;
import com.orientechnologies.orient.server.distributed.OLocalDHTNode;
import com.orientechnologies.orient.server.distributed.ONodeAddress;
import com.orientechnologies.orient.server.distributed.ONodeOfflineException;
import com.orientechnologies.orient.server.distributed.Record;
import com.orientechnologies.orient.server.distributed.operations.ORemoteNodeCallResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.orientechnologies.orient.core.id.OClusterPositionNodeId;
import com.orientechnologies.orient.core.id.ONodeId;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.storage.ORecordDuplicatedException;

/**
 * @author Andrey Lomakin
 * @since 21.11.12
 */
public class ORecordCreatorImpl implements ORecordCreator {
  private static final int     MAX_RETRIES = 10;

  private static final Logger  LOGGER      = LoggerFactory.getLogger(OLocalDHTNode.class);

  private final ODHTNode node;
  private final ODHTNodeLookup nodeLookup;

  public ORecordCreatorImpl(ODHTNode node, ODHTNodeLookup nodeLookup) {
    this.node = node;
    this.nodeLookup = nodeLookup;
  }

  @Override
  public Record createRecord(String data) {
    waitTillJoin();

    int retryCount = 0;

    while (true) {
      try {
        final ONodeId id = ONodeId.generateUniqueId();
        retryCount++;

        return createRecord(new ORecordId(1, new OClusterPositionNodeId(id)), data);
      } catch (ORecordDuplicatedException e) {
        // ignore
        if (retryCount >= MAX_RETRIES)
          throw e;
      }
    }
  }

  @Override
  public Record createRecord(ORecordId recordId, String data) {
    waitTillJoin();
    int retryCount = 0;

    while (true) {
      LOGGER.debug("Looking for successor for record {}", recordId);

      final ONodeAddress successorAddress = node.findSuccessor(((OClusterPositionNodeId) recordId.clusterPosition).getNodeId());

      LOGGER.debug("Successor for record {} is {}", recordId, successorAddress);
      retryCount++;

      if (successorAddress.equals(node.getNodeAddress())) {
        return node.createRecordInNode(recordId, data);
      } else {
        final ORemoteNodeCallResult<Record> result = remoteNodeCreate(recordId, data, retryCount, successorAddress);
        if (result.isRepeat()) {
          LOGGER.error("Successor {} for record {} is offline, retry.", successorAddress, recordId);
          node.requestStabilization();
          continue;
        }

        return result.getData();
      }
    }
  }

  private ORemoteNodeCallResult<Record> remoteNodeCreate(ORecordId id, String data, int retryCount, ONodeAddress remoteNodeAddress) {
    Logger logger = LoggerFactory.getLogger(OLocalDHTNode.class);

    final ODHTNode node = nodeLookup.findById(remoteNodeAddress);

    if (node == null) {
      if (retryCount < MAX_RETRIES) {
        logger.debug("Node {} is offline, retry {}-d time.", remoteNodeAddress, retryCount);

        try {
          Thread.sleep(50);
        } catch (InterruptedException e) {
          logger.error("Thread execution was interrupted.", e);
          throw new ORemoteNodeCallException("Thread execution was interrupted.", e, remoteNodeAddress.getNodeId());
        }

        return new ORemoteNodeCallResult<Record>(true, null);
      } else {
        logger.error("Node {} is offline, retry limit is reached.", remoteNodeAddress);
        throw new ONodeOfflineException("Node " + remoteNodeAddress + " is offline, retry limit is reached.", null,
            remoteNodeAddress.getNodeId());
      }
    }

    try {
      return new ORemoteNodeCallResult<Record>(false, node.createRecordInNode(id, data));
    } catch (ONodeOfflineException ooe) {
      if (retryCount < MAX_RETRIES) {
        logger.debug("Node {} is offline, retry {}-d time.", remoteNodeAddress, retryCount);

        try {
          Thread.sleep(50);
        } catch (InterruptedException e) {
          logger.error("Thread execution was interrupted.", e);
          throw new ORemoteNodeCallException("Thread execution was interrupted.", e, remoteNodeAddress.getNodeId());
        }

        return new ORemoteNodeCallResult<Record>(true, null);
      } else {
        logger.error("Node " + remoteNodeAddress + " is offline, retry limit is reached.");
        throw new ONodeOfflineException("Node " + remoteNodeAddress + " is offline, retry limit is reached.", null,
            remoteNodeAddress.getNodeId());
      }
    }
  }

  private void waitTillJoin() {
    while (!ODHTNode.NodeState.PRODUCTION.equals(node.state())) {
      LOGGER.info("Wait till node {} will be joined.", node.getNodeAddress());
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException(e);
      }
    }
  }

}
