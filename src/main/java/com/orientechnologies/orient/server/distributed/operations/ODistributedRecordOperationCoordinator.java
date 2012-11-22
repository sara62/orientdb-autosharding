package com.orientechnologies.orient.server.distributed.operations;

import com.orientechnologies.orient.server.distributed.ODHTNode;

/**
 * @author Andrey Lomakin
 * @since 21.11.12
 */
public interface ODistributedRecordOperationCoordinator {
  public <T> T executeRecordOperation(ODHTNode node, ODistributedRecordOperation<T> recordOperation);
}
