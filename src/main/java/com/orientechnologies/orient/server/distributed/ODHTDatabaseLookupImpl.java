package com.orientechnologies.orient.server.distributed;

import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.OServerMain;
import com.orientechnologies.orient.server.config.OServerUserConfiguration;

/**
 * @author <a href="mailto:enisher@gmail.com">Artem Orobets</a>
 * @since 12/21/12
 */
public class ODHTDatabaseLookupImpl implements ODHTDatabaseLookup {
  public static final String DEFAULT_REPLICATOR_USER_NAME = "replicator";

  private final String databaseName;
  private String userName;
  private String password;

  public ODHTDatabaseLookupImpl(String databaseName) {
    this.databaseName = databaseName;
    final OServerUserConfiguration replicatorUser = OServerMain.server().getUser(DEFAULT_REPLICATOR_USER_NAME);
    userName = replicatorUser.name;
    password = replicatorUser.password;
  }

  public ODHTDatabaseLookupImpl(String databaseName, String userName, String password) {
    this.databaseName = databaseName;
    this.userName = userName;
    this.password = password;
  }

  @Override
  //TODO move to storage name
  public ODatabaseRecord openDatabase(String storageName) {
    final ODatabaseRecord db = ODatabaseRecordThreadLocal.INSTANCE.getIfDefined();
    if (db != null && db.getName().equals(databaseName) && !db.isClosed()) {
      if (db instanceof ODatabaseDocumentTx)
        return db;
      else if (db.getDatabaseOwner() instanceof ODatabaseDocumentTx)
        return (ODatabaseDocumentTx) db.getDatabaseOwner();
    }

    final OServer server = OServerMain.server();
    if (server != null) {
      return (ODatabaseDocumentTx) server.openDatabase("document", databaseName, userName, password);
    } else {
      final ODatabaseDocumentTx database = new ODatabaseDocumentTx(databaseName);
      database.open(userName, password);
      return database;
    }
  }
}
