package com.orientechnologies.orient.server.distributed.ringprotocols.crud;

import java.util.Set;

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.server.distributed.ODHTNodeLocal;
import com.orientechnologies.orient.server.distributed.ONodeAddress;

/**
 * @author Andrey Lomakin
 * @since 22.11.12
 */
public interface ORecordMergeStrategy {
  public void mergeReplicaVersions(ODHTNodeLocal localNode, ORID recordId, Set<ONodeAddress> replicaHolders);
}
