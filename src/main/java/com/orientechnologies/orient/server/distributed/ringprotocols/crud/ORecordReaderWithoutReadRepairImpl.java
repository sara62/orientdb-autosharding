package com.orientechnologies.orient.server.distributed.ringprotocols.crud;

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.server.distributed.ODHTNodeLocal;
import com.orientechnologies.orient.server.distributed.Record;
import com.orientechnologies.orient.server.distributed.util.OWaitTillNodeJoin;

/**
 * @author Andrey Lomakin
 * @since 23.11.12
 */
public class ORecordReaderWithoutReadRepairImpl implements ORecordReader {
	public ORecordReaderWithoutReadRepairImpl() {
	}

	@Override
	public Record readRecord(ODHTNodeLocal nodeLocal, ORID recordId) {
		OWaitTillNodeJoin.waitTillNodeJoin(nodeLocal);
		return nodeLocal.readRecordLocal(recordId);
	}
}