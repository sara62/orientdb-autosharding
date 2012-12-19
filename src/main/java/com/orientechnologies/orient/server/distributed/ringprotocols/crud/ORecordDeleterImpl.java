package com.orientechnologies.orient.server.distributed.ringprotocols.crud;

import com.orientechnologies.orient.core.version.ORecordVersion;
import com.orientechnologies.orient.server.distributed.ODHTNodeLocal;
import com.orientechnologies.orient.server.distributed.util.OWaitTillNodeJoin;

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.server.distributed.ringprotocols.ORecordReplicator;

/**
 * @author Andrey Lomakin
 * @since 22.11.12
 */
public final class ORecordDeleterImpl implements ORecordDeleter {
  private final ORecordReplicator recordReplicator;

  public ORecordDeleterImpl(ORecordReplicator recordReplicator) {
    this.recordReplicator = recordReplicator;
  }

  @Override
  public void deleteRecord(String storageName, ODHTNodeLocal localNode, ORID recordId, ORecordVersion version,
													 int replicaCount, int syncReplicaCount) {
		OWaitTillNodeJoin.waitTillNodeJoin(localNode);

    localNode.removeRecordLocal(storageName, recordId, version);
    recordReplicator.replicateRecord(localNode, storageName, recordId, replicaCount, syncReplicaCount);
  }
}
