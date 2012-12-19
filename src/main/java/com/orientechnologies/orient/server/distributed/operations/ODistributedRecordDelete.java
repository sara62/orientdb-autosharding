package com.orientechnologies.orient.server.distributed.operations;

import com.orientechnologies.orient.core.id.OClusterPosition;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.version.ORecordVersion;
import com.orientechnologies.orient.server.distributed.ODHTNode;

/**
 * @author Andrey Lomakin
 * @since 21.11.12
 */
public final class ODistributedRecordDelete implements ODistributedRecordOperation<Void> {
  private final ORID           recordId;
  private final ORecordVersion version;
	private final String         storageName;

  public ODistributedRecordDelete(String storageName, ORID recordId, ORecordVersion version) {
		this.storageName = storageName;
		this.recordId = recordId;
    this.version = version;
  }

  @Override
  public Void execute(ODHTNode node) {
    node.deleteRecordFromNode(storageName, recordId, version);

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
