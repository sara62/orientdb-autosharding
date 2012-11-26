package com.orientechnologies.orient.server.distributed.operations;

import com.orientechnologies.orient.core.id.OClusterPosition;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.server.distributed.ODHTNode;
import com.orientechnologies.orient.server.distributed.ODHTRecordVersion;

/**
 * @author Andrey Lomakin
 * @since 21.11.12
 */
public class ODistributedRecordDelete implements ODistributedRecordOperation<Void> {
  private final ORID         recordId;
  private final ODHTRecordVersion version;

  public ODistributedRecordDelete(ORID recordId, ODHTRecordVersion version) {
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
