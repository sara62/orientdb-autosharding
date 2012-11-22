package com.orientechnologies.orient.server.distributed.operations;

import com.orientechnologies.orient.core.id.OClusterPosition;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.server.distributed.ODHTNode;
import com.orientechnologies.orient.server.distributed.Record;

/**
 * @author Andrey Lomakin
 * @since 21.11.12
 */
public final class ODistributedRecordUpdate implements ODistributedRecordOperation<Void> {
  private final Record record;

  public ODistributedRecordUpdate(Record record) {
    this.record = record;
  }

  @Override
  public Void execute(ODHTNode node) {
    node.updateRecordInNode(record.getId(), record);

    return null;
  }

  @Override
  public OClusterPosition getClusterPosition() {
    return record.getId().getClusterPosition();
  }

  @Override
  public int getClusterId() {
    return record.getId().clusterId;
  }

  @Override
  public void setRecordId(ORecordId recordId) {
    throw new UnsupportedOperationException("setRecordId");
  }
}
