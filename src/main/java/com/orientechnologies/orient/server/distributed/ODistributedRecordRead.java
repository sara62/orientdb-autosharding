package com.orientechnologies.orient.server.distributed;

import com.orientechnologies.orient.core.id.OClusterPosition;
import com.orientechnologies.orient.core.id.ORecordId;

/**
 * @author Andrey Lomakin
 * @since 21.11.12
 */
public class ODistributedRecordRead implements ODistributedRecordOperation<Record> {
  private final ORecordId recordId;

  public ODistributedRecordRead(ORecordId recordId) {
    this.recordId = recordId;
  }

  @Override
  public Record execute(ODHTNode node) {
    return node.getRecordFromNode(recordId, true);
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
