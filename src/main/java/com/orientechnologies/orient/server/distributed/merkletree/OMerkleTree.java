package com.orientechnologies.orient.server.distributed.merkletree;

import java.util.List;

import com.orientechnologies.orient.core.id.ONodeId;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.server.distributed.ODHTRecordVersion;
import com.orientechnologies.orient.server.distributed.Record;

/**
 * @author Andrey Lomakin
 * @since 13.09.12
 */
public interface OMerkleTree {
  public Record addData(final ORecordId id, final String data);

  public void deleteData(final ORecordId id, final ODHTRecordVersion version);

  public void deleteData(final ORecordId id, final ODHTRecordVersion version, boolean softDelete);

  public void updateData(final ORecordId id, final ODHTRecordVersion version, final String data);

  public void updateReplica(final ORecordId id, final Record replica);

  public ODetachedMerkleTreeNode getRootNode(int index);

  public List<ODetachedMerkleTreeNode> getRootNodesForInterval(final ONodeId startId, final ONodeId endId);

  public ODetachedMerkleTreeNode getEquivalentNode(ODetachedMerkleTreeNode detachedNode);

  public ODetachedMerkleTreeNode getChildNode(ODetachedMerkleTreeNode parent, int index);
}
