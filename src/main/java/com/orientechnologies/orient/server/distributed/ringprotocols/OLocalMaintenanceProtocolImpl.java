package com.orientechnologies.orient.server.distributed.ringprotocols;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.orientechnologies.orient.core.id.ONodeId;
import com.orientechnologies.orient.server.distributed.ODHTNode;
import com.orientechnologies.orient.server.distributed.ODHTNodeLocal;
import com.orientechnologies.orient.server.distributed.ODHTNodeLookup;
import com.orientechnologies.orient.server.distributed.ODHTRingInterval;
import com.orientechnologies.orient.server.distributed.ONodeAddress;
import com.orientechnologies.orient.server.distributed.merkletree.ODetachedMerkleTreeNode;
import com.orientechnologies.orient.server.distributed.merkletree.OMerkleTree;
import com.orientechnologies.orient.server.distributed.util.OWaitTillNodeJoin;

/**
 * @author Andrey Lomakin
 * @since 23.11.12
 */
public final class OLocalMaintenanceProtocolImpl implements OLocalMaintenanceProtocol {
  private final Logger                                    logger = LoggerFactory.getLogger(OLocalMaintenanceProtocolImpl.class);

  private final OReplicaDistributionStrategy              replicaDistributionStrategy;
  private final ODHTNodeLookup                            nodeLookup;
  private final OLocalMaintenanceProtocolNodeComparator[] nodeComparators;

  public OLocalMaintenanceProtocolImpl(OReplicaDistributionStrategy replicaDistributionStrategy, ODHTNodeLookup nodeLookup,
      OLocalMaintenanceProtocolNodeComparator[] nodeComparators) {
    this.replicaDistributionStrategy = replicaDistributionStrategy;
    this.nodeLookup = nodeLookup;
    this.nodeComparators = nodeComparators;
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

  private void synchronizeNode(final ODHTNodeLocal localDHTNode, final ODetachedMerkleTreeNode localTreeNode,
      final ONodeAddress remoteNodeAddress) {
    final ODHTNode remoteNode = nodeLookup.findById(remoteNodeAddress);

    if (remoteNode == null)
      throw new OLocalProtocolException("Node with id " + remoteNodeAddress + " is absent.");

    final ODetachedMerkleTreeNode remoteTreeNode = remoteNode.findMerkleTreeNode(localTreeNode);

    if (remoteTreeNode == null)
      throw new OLocalProtocolException("Related remote Merkle tree node is null.");

    final ONodeAddress localPredecessor = localDHTNode.getPredecessor();
    if (localPredecessor == null)
      throw new OLocalProtocolException("Predecessor " + localPredecessor + " is absent.");

    if (Arrays.equals(remoteTreeNode.getHash(), localTreeNode.getHash()))
      return;

    for (OLocalMaintenanceProtocolNodeComparator nodeComparator : nodeComparators)
      nodeComparator.compareNodes(localDHTNode, localTreeNode, remoteTreeNode, remoteNodeAddress);

    final ODHTRingInterval nodeInterval = new ODHTRingInterval(localPredecessor.getNodeId().add(ONodeId.ONE), localDHTNode
        .getNodeAddress().getNodeId());

    if (!localTreeNode.isLeaf() && !remoteTreeNode.isLeaf()) {
      for (int i = 0; i < 64; i++) {
        final ODetachedMerkleTreeNode childTreeNode = localDHTNode.getLocalMerkleTree().getChildNode(localTreeNode, i);
        if (childTreeNode == null)
          throw new OLocalProtocolException("Children of Merklee tree node were removed.");

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
}
