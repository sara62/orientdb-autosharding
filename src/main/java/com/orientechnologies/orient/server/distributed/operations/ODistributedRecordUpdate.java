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
public final class ODistributedRecordUpdate implements ODistributedRecordOperation<ORecordInternal<?>> {
	private final String storageName;
  private final ORecordInternal<?> record;

  public ODistributedRecordUpdate(String storageName, ORecordInternal<?> record) {
		this.storageName = storageName;
		this.record = record;
  }

  @Override
  public ORecordInternal<?> execute(ODHTNode node) {
    return node.updateRecordInNode(storageName, record);
  }

  @Override
  public OClusterPosition getClusterPosition() {
    return record.getIdentity().getClusterPosition();
  }

  @Override
  public int getClusterId() {
    return record.getIdentity().getClusterId();
  }

  @Override
  public void setRecordId(ORID recordId) {
    throw new UnsupportedOperationException("setRecordId");
  }
}
