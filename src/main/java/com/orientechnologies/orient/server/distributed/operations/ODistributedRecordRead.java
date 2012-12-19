package com.orientechnologies.orient.server.distributed.operations;

import com.orientechnologies.orient.core.id.OClusterPosition;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.server.distributed.ODHTNode;
import com.orientechnologies.orient.server.distributed.Record;

/**
 * @author Andrey Lomakin
 * @since 21.11.12
 */
public final class ODistributedRecordRead implements ODistributedRecordOperation<ORecordInternal<?>> {
	private final String storageName;
  private final ORID recordId;

  public ODistributedRecordRead(String storageName, ORID recordId) {
		this.storageName = storageName;
		this.recordId = recordId;
  }

  @Override
  public ORecordInternal<?> execute(ODHTNode node) {
    return node.readRecordFromNode(storageName, recordId);
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
