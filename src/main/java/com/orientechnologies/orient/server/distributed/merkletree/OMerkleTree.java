package com.orientechnologies.orient.server.distributed.merkletree;

import java.util.List;

import com.orientechnologies.orient.core.id.ONodeId;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.version.ORecordVersion;

/**
 * @author Andrey Lomakin
 * @since 13.09.12
 */
public interface OMerkleTree {
  public ORecordInternal<?> addData(final ORID id, final ORecordInternal<?> data);

  public void deleteData(final ORID id, final ORecordVersion version);

  public void deleteData(final ORID id, final ORecordVersion version, boolean softDelete);

  public void updateData(final ORID id, final ORecordVersion version, final ORecordInternal<?> data);

  public void updateReplica(final ORID id, final ORecordInternal<?> replica);

  public ODetachedMerkleTreeNode getRootNode(int index);

  public List<ODetachedMerkleTreeNode> getRootNodesForInterval(final ONodeId startId, final ONodeId endId);

  public ODetachedMerkleTreeNode getEquivalentNode(ODetachedMerkleTreeNode detachedNode);

  public ODetachedMerkleTreeNode getChildNode(ODetachedMerkleTreeNode parent, int index);
}
