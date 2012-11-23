package com.orientechnologies.orient.server.distributed;

import com.orientechnologies.common.concur.lock.OLockManager;
import com.orientechnologies.orient.core.id.ORecordId;

/**
 * @author Andrey Lomakin
 * @since 22.11.12
 */
public interface ODHTNodeLocal extends ODHTNode {
	public Record addData(ORecordId id, String data);
	public void updateData(ORecordId id, Record record);
	public Record readData(ORecordId recordId);
	public void removeData(ORecordId id, ODHTRecordVersion version);
}
