package com.orientechnologies.orient.server.distributed.operations;

import com.orientechnologies.orient.core.id.OClusterPosition;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.server.distributed.ODHTNode;
import com.orientechnologies.orient.server.distributed.Record;

/**
 * @author Andrey Lomakin
 * @since 21.11.12
 */
public final class ODistributedRecordCreation implements ODistributedRecordOperation<Record> {
  private final String data;
  private ORecordId    recordId;

  public ODistributedRecordCreation(String data, ORecordId recordId) {
    this.data = data;
    this.recordId = recordId;
  }

  @Override
  public OClusterPosition getClusterPosition() {
    if (recordId == null)
      return null;

    return recordId.clusterPosition;
  }

  @Override
  public int getClusterId() {
    return 1;
  }

  public void setRecordId(ORecordId recordId) {
    this.recordId = recordId;
  }

  @Override
  public Record execute(ODHTNode node) {
    return node.createRecordInNode(recordId, data);
  }
}
