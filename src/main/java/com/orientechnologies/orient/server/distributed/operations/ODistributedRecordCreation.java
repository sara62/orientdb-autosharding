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
public final class ODistributedRecordCreation implements ODistributedRecordOperation<ORecordInternal<?>> {
  private final ORecordInternal<?> recordInternal;
	private final String storageName;

  public ODistributedRecordCreation(String storageName, ORecordInternal<?> recordInternal) {
    this.recordInternal = recordInternal;
		this.storageName = storageName;
  }

  @Override
  public OClusterPosition getClusterPosition() {
    return recordInternal.getIdentity().getClusterPosition();
  }

  @Override
  public int getClusterId() {
    return recordInternal.getIdentity().getClusterId();
  }

  public void setRecordId(ORID recordId) {
    recordInternal.setIdentity(recordId.getClusterId(), recordId.getClusterPosition());
  }

  @Override
  public ORecordInternal<?> execute(ODHTNode node) {
    return node.createRecordInNode(storageName, recordInternal);
  }
}
