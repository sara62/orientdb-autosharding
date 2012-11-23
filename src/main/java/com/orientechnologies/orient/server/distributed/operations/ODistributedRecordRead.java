package com.orientechnologies.orient.server.distributed.operations;

import com.orientechnologies.orient.core.id.OClusterPosition;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.server.distributed.ODHTNode;
import com.orientechnologies.orient.server.distributed.Record;

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
    return node.readRecordFromNode(recordId);
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
