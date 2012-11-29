package com.orientechnologies.orient.server.distributed.ringprotocols.crud;

import java.util.Set;

import com.orientechnologies.orient.server.distributed.ODHTNodeLocal;
import com.orientechnologies.orient.server.distributed.util.OWaitTillNodeJoin;
import com.orientechnologies.orient.server.distributed.ringprotocols.OReplicaDistributionStrategy;

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.server.distributed.ONodeAddress;
import com.orientechnologies.orient.server.distributed.Record;


/**
 * @author Andrey Lomakin
 * @since 22.11.12
 */
public class ORecordReaderImpl implements ORecordReader {
  private final OReplicaDistributionStrategy replicaDistributionStrategy;
	private final ORecordMergeStrategy         recordMergeStrategy;

  private final int                          replicaCount;
  private final int                          syncReplicaCount;

  public ORecordReaderImpl(OReplicaDistributionStrategy replicaDistributionStrategy,
													 ORecordMergeStrategy recordMergeStrategy,
													 int replicaCount, int syncReplicaCount) {
    this.replicaDistributionStrategy = replicaDistributionStrategy;
		this.recordMergeStrategy = recordMergeStrategy;
		this.replicaCount = replicaCount;
    this.syncReplicaCount = syncReplicaCount;
	}

  @Override
  public Record readRecord(ODHTNodeLocal nodeLocal, ORID recordId, int replicaCount, int syncReplicaCount) {
		OWaitTillNodeJoin.waitTillNodeJoin(nodeLocal);

		final ONodeAddress[] successors = nodeLocal.getSuccessors();

    final Set<ONodeAddress>[] replicas = replicaDistributionStrategy.chooseReplicas(successors,
						this.replicaCount, this.syncReplicaCount);

    final Set<ONodeAddress> syncReplicas = replicas[0];
    final Set<ONodeAddress> asyncReplicas = replicas[1];

		recordMergeStrategy.mergeReplicaVersions(nodeLocal, recordId, syncReplicas);

    final Record result = nodeLocal.readRecordLocal(recordId);

    if (!asyncReplicas.isEmpty())
			new OReadRepairTask(recordId, asyncReplicas, nodeLocal, recordMergeStrategy).submit();

		if (result.isTombstone())
			return null;

    return result;
  }
}
