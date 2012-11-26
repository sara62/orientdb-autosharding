package com.orientechnologies.orient.server.distributed.merkletree;

import java.util.List;

import com.orientechnologies.orient.core.id.ONodeId;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.server.distributed.ODHTRecordVersion;
import com.orientechnologies.orient.server.distributed.Record;

/**
 * @author Andrey Lomakin
 * @since 13.09.12
 */
public interface OMerkleTree {
  public Record addData(final ORID id, final String data);

  public void deleteData(final ORID id, final ODHTRecordVersion version);

  public void deleteData(final ORID id, final ODHTRecordVersion version, boolean softDelete);

  public void updateData(final ORID id, final ODHTRecordVersion version, final String data);

  public void updateReplica(final ORID id, final Record replica);

  public ODetachedMerkleTreeNode getRootNode(int index);

  public List<ODetachedMerkleTreeNode> getRootNodesForInterval(final ONodeId startId, final ONodeId endId);

  public ODetachedMerkleTreeNode getEquivalentNode(ODetachedMerkleTreeNode detachedNode);

  public ODetachedMerkleTreeNode getChildNode(ODetachedMerkleTreeNode parent, int index);
}
