package com.orientechnologies.orient.server.distributed.operations;

import com.orientechnologies.orient.core.id.OClusterPosition;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.server.distributed.ODHTNode;
import com.orientechnologies.orient.server.distributed.Record;

/**
 * @author Andrey Lomakin
 * @since 21.11.12
 */
public final class ODistributedRecordRead implements ODistributedRecordOperation<Record> {
  private final ORID recordId;

  public ODistributedRecordRead(ORID recordId) {
    this.recordId = recordId;
  }

  @Override
  public Record execute(ODHTNode node) {
    return node.readRecordFromNode(recordId);
  }

  @Override
  public OClusterPosition getClusterPosition() {
    return recordId.getClusterPosition();
  }

  @Override
  public int getClusterId() {
    return recordId.getClusterId();
  }

  @Override
  public void setRecordId(ORID recordId) {
    throw new UnsupportedOperationException("setRecordId");
  }
}
