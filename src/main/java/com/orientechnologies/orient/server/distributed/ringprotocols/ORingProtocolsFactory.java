package com.orientechnologies.orient.server.distributed.ringprotocols;

import com.orientechnologies.orient.server.distributed.ODHTNodeLocal;
import com.orientechnologies.orient.server.distributed.ODHTNodeLookup;
import com.orientechnologies.orient.server.distributed.ringprotocols.crud.ORecordCreator;
import com.orientechnologies.orient.server.distributed.ringprotocols.crud.ORecordDeleter;
import com.orientechnologies.orient.server.distributed.ringprotocols.crud.ORecordMergeStrategy;
import com.orientechnologies.orient.server.distributed.ringprotocols.crud.ORecordReader;
import com.orientechnologies.orient.server.distributed.ringprotocols.crud.ORecordUpdater;

/**
 * @author Andrey Lomakin
 * @since 22.11.12
 */
public interface ORingProtocolsFactory {
  public ORecordReplicator createRecordReplicator(ODHTNodeLookup nodeLookup);

  public OReplicaDistributionStrategy createReplicaDistributionStrategy();

  public ORecordMergeStrategy createRecordMergeStrategy(ODHTNodeLookup nodeLookup);

  public ORecordCreator createRecordCreator(ODHTNodeLookup nodeLookup, int replicaCount, int syncReplicaCount);

  public ORecordUpdater createRecordUpdater(ODHTNodeLookup nodeLookup, int replicaCount, int syncReplicaCount);

  public ORecordDeleter createRecordDeleter(ODHTNodeLookup nodeLookup, int replicaCount, int syncReplicaCount);

  public ORecordReader createRecordReader(ODHTNodeLookup nodeLookup, int replicaCount, int syncReplicaCount);

  public OLocalMaintenanceProtocol createLocalMaintenanceProtocol(ODHTNodeLookup nodeLookup);
  public OGlobalMaintenanceProtocol createGlobalMaintenanceProtocol(ODHTNodeLookup nodeLookup);
}
