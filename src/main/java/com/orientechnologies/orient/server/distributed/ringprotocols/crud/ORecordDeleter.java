package com.orientechnologies.orient.server.distributed.ringprotocols.crud;

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.version.ORecordVersion;
import com.orientechnologies.orient.server.distributed.ODHTNodeLocal;

/**
 * @author Andrey Lomakin
 * @since 22.11.12
 */
public interface ORecordDeleter {
	public void deleteRecord(String storageName, ODHTNodeLocal localNode, ORID recordId, ORecordVersion version,
													 int replicaCount, int syncReplicaCount);
}
