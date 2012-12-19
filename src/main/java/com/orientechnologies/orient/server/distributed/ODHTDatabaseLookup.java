package com.orientechnologies.orient.server.distributed;

import com.orientechnologies.orient.core.db.record.ODatabaseRecord;

/**
 * @author Andrey Lomakin
 * @since 05.12.12
 */
public interface ODHTDatabaseLookup {
	ODatabaseRecord openDatabase(String storageName);
}
