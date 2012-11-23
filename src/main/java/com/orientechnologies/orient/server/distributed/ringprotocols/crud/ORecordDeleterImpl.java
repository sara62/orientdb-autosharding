package com.orientechnologies.orient.server.distributed.ringprotocols.crud;

import com.orientechnologies.orient.server.distributed.ODHTNodeLocal;
import com.orientechnologies.orient.server.distributed.util.OWaitTillNodeJoin;

import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.server.distributed.ODHTRecordVersion;
import com.orientechnologies.orient.server.distributed.ringprotocols.ORecordReplicator;

/**
 * @author Andrey Lomakin
 * @since 22.11.12
 */
public class ORecordDeleterImpl implements ORecordDeleter {
  private final ORecordReplicator recordReplicator;
  private final int               replicaCount;
  private final int               syncReplicaCount;

  public ORecordDeleterImpl(ORecordReplicator recordReplicator, int replicaCount, int syncReplicaCount) {
    this.recordReplicator = recordReplicator;
    this.replicaCount = replicaCount;
    this.syncReplicaCount = syncReplicaCount;
  }

  @Override
  public void deleteRecord(ODHTNodeLocal localNode, ORecordId recordId, ODHTRecordVersion version) {
		OWaitTillNodeJoin.waitTillNodeJoin(localNode);

    localNode.removeRecordLocal(recordId, version);
    recordReplicator.replicateRecord(localNode, recordId, replicaCount, syncReplicaCount);
  }
}
