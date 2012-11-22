package com.orientechnologies.orient.server.distributed.operations;

import com.orientechnologies.orient.core.id.OClusterPosition;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.server.distributed.ODHTNode;

/**
 * @author Andrey Lomakin
 * @since 21.11.12
 */
public interface ODistributedRecordOperation<T> {
  public T execute(final ODHTNode node);

  public OClusterPosition getClusterPosition();

  public int getClusterId();

  public void setRecordId(ORecordId recordId);
}
