package com.orientechnologies.orient.server.distributed.operations;

import com.orientechnologies.orient.core.id.OClusterPosition;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.server.distributed.ODHTNode;
import com.orientechnologies.orient.server.distributed.ODHTRecordVersion;

/**
 * @author Andrey Lomakin
 * @since 21.11.12
 */
public class ODistributedRecordDelete implements ODistributedRecordOperation<Void> {
  private final ORecordId         recordId;
  private final ODHTRecordVersion version;

  public ODistributedRecordDelete(ORecordId recordId, ODHTRecordVersion version) {
    this.recordId = recordId;
    this.version = version;
  }

  @Override
  public Void execute(ODHTNode node) {
    node.deleteRecordFromNode(recordId, version);

    return null;
  }

  @Override
  public OClusterPosition getClusterPosition() {
    return recordId.clusterPosition;
  }

  @Override
  public int getClusterId() {
    return recordId.clusterId;
  }

  @Override
  public void setRecordId(ORecordId recordId) {
    throw new UnsupportedOperationException("setRecordId");
  }
}
