package com.orientechnologies.orient.server.distributed.operations;

import com.orientechnologies.orient.server.distributed.ODHTNodeLookup;
import com.orientechnologies.orient.server.distributed.operations.ODistributedCoordinatorFactory;
import com.orientechnologies.orient.server.distributed.operations.ODistributedRecordOperationCoordinator;
import com.orientechnologies.orient.server.distributed.operations.ODistributedRecordOperationCoordinatorImpl;

/**
 * @author Andrey Lomakin
 * @since 21.11.12
 */
public class ODefaultDistributedCoordinatorFactory implements ODistributedCoordinatorFactory {
  @Override
  public ODistributedRecordOperationCoordinator createOperationCoordinator(ODHTNodeLookup nodeLookup) {
    return new ODistributedRecordOperationCoordinatorImpl(nodeLookup);
  }
}
