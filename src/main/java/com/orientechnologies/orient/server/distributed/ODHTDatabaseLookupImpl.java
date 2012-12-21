package com.orientechnologies.orient.server.distributed;

import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.server.OServerMain;
import com.orientechnologies.orient.server.config.OServerUserConfiguration;

/**
 * @author <a href="mailto:enisher@gmail.com">Artem Orobets</a>
 * @since 12/21/12
 */
public class ODHTDatabaseLookupImpl implements ODHTDatabaseLookup {
  public static final String                    REPLICATOR_USER = "replicator";

  private static final OServerUserConfiguration replicatorUser  = OServerMain.server().getUser(REPLICATOR_USER);
  private final String databaseName;

  public ODHTDatabaseLookupImpl(String databaseName) {
    this.databaseName = databaseName;
  }

  @Override
  public ODatabaseRecord openDatabase(String storageName) {
    final ODatabaseRecord db = ODatabaseRecordThreadLocal.INSTANCE.getIfDefined();
    if (db != null && db.getName().equals(databaseName) && !db.isClosed()) {
      if (db instanceof ODatabaseDocumentTx)
        return db;
      else if (db.getDatabaseOwner() instanceof ODatabaseDocumentTx)
        return (ODatabaseDocumentTx) db.getDatabaseOwner();
    }

    return (ODatabaseDocumentTx) OServerMain.server().openDatabase("document", databaseName, replicatorUser.name,
        replicatorUser.password);
  }
}
