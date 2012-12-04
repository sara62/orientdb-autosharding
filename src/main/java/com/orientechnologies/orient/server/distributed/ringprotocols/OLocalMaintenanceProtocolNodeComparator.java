package com.orientechnologies.orient.server.distributed.ringprotocols;

import com.orientechnologies.orient.server.distributed.ODHTNodeLocal;
import com.orientechnologies.orient.server.distributed.ONodeAddress;
import com.orientechnologies.orient.server.distributed.merkletree.ODetachedMerkleTreeNode;

/**
 * @author Andrey Lomakin
 * @since 30.11.12
 */
public interface OLocalMaintenanceProtocolNodeComparator {
	public void compareNodes(ODHTNodeLocal localDHTNode,
													 ODetachedMerkleTreeNode localTreeNode, ODetachedMerkleTreeNode remoteNode,
													 ONodeAddress remoteNodeAddress);
}