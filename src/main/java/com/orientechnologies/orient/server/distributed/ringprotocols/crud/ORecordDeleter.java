package com.orientechnologies.orient.server.distributed.ringprotocols.crud;

import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.server.distributed.ODHTNodeLocal;
import com.orientechnologies.orient.server.distributed.ODHTRecordVersion;

/**
 * @author Andrey Lomakin
 * @since 22.11.12
 */
public interface ORecordDeleter {
	public void deleteRecord(ODHTNodeLocal localNode, ORecordId recordId, ODHTRecordVersion version);
}
