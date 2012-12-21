package com.orientechnologies.orient.server.distributed.ringprotocols.crud;

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.server.distributed.ODHTNodeLocal;
import com.orientechnologies.orient.server.distributed.ONodeAddress;

import java.util.Set;

/**
 * @author Andrey Lomakin
 * @since 05.12.12
 */
public interface OReadRepairExecutor {
	void submit(String storageName, ORID recordId, Set<ONodeAddress> replicaHolders, ODHTNodeLocal localNode,
              ORecordMergeExecutionContext executionContext);
}
