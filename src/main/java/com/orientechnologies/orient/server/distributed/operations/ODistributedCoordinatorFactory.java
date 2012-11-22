package com.orientechnologies.orient.server.distributed.operations;

import com.orientechnologies.orient.server.distributed.ODHTNodeLookup;

/**
 * @author Andrey Lomakin
 * @since 21.11.12
 */
public interface ODistributedCoordinatorFactory {
  public ODistributedRecordOperationCoordinator createOperationCoordinator(ODHTNodeLookup nodeLookup);
}
