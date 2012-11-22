package com.orientechnologies.orient.server.distributed;

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
