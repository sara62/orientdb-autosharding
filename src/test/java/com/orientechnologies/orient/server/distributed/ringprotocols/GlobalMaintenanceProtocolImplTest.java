package com.orientechnologies.orient.server.distributed.ringprotocols;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.id.OClusterPositionNodeId;
import com.orientechnologies.orient.core.id.ONodeId;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.server.distributed.ODHTNode;
import com.orientechnologies.orient.server.distributed.ODHTNodeLocal;
import com.orientechnologies.orient.server.distributed.ODHTNodeLookup;
import com.orientechnologies.orient.server.distributed.ODHTRecordVersion;
import com.orientechnologies.orient.server.distributed.ONodeAddress;
import com.orientechnologies.orient.server.distributed.ONodeOfflineException;
import com.orientechnologies.orient.server.distributed.ORecordMetadata;
import com.orientechnologies.orient.server.distributed.Record;

/**
 * @author Andrey Lomakin
 * @since 27.11.12
 */
@Test
public class GlobalMaintenanceProtocolImplTest {
  @Mock
  private ODHTNodeLookup               nodeLookup;

  @Mock
  private OReplicaDistributionStrategy replicaDistributionStrategy;

  private OGlobalMaintenanceProtocol   globalMaintenanceProtocol;

  @Mock
  private ODHTNodeLocal                nodeLocal;

  @BeforeMethod
  public void setUp() {
    MockitoAnnotations.initMocks(this);

    globalMaintenanceProtocol = new OGlobalMaintenanceProtocolImpl(nodeLookup, replicaDistributionStrategy);
  }

  public void testReallocateWrongPlacedReplicasStateIsNull() {
    final ONodeId idToStart = ONodeId.generateUniqueId();
    final ONodeId nodeId = ONodeId.generateUniqueId();

    final ONodeAddressStub nodeAddressStub = new ONodeAddressStub(nodeId);

    when(nodeLocal.state()).thenReturn(null);
    when(nodeLocal.getNodeAddress()).thenReturn(nodeAddressStub);

    final ONodeId result = globalMaintenanceProtocol.reallocateWrongPlacedReplicas(nodeLocal, idToStart, 1, 1);

    Assert.assertEquals(result, nodeId);

    verify(nodeLocal).state();
    verify(nodeLocal).getNodeAddress();

    verifyZeroInteractions(replicaDistributionStrategy, nodeLookup);
    verifyNoMoreInteractions(nodeLocal);
  }

  public void testReallocateWrongPlacedReplicasStateNotInProduction() {
    final ONodeId idToStart = ONodeId.generateUniqueId();
    final ONodeId nodeId = ONodeId.generateUniqueId();

    final ONodeAddressStub nodeAddressStub = new ONodeAddressStub(nodeId);

    when(nodeLocal.state()).thenReturn(ODHTNode.NodeState.JOIN);
    when(nodeLocal.getNodeAddress()).thenReturn(nodeAddressStub);

    final ONodeId result = globalMaintenanceProtocol.reallocateWrongPlacedReplicas(nodeLocal, idToStart, 1, 1);

    Assert.assertEquals(result, nodeId);

    verify(nodeLocal).state();
    verify(nodeLocal).getNodeAddress();

    verifyZeroInteractions(replicaDistributionStrategy, nodeLookup);
    verifyNoMoreInteractions(nodeLocal);
  }

  public void testReallocateWrongPlacedReplicasStateNoItemsInDB() {
    final ONodeId idToStart = ONodeId.generateUniqueId();
    final ONodeId nodeId = ONodeId.generateUniqueId();

    final ONodeAddressStub nodeAddressStub = new ONodeAddressStub(nodeId);

    when(nodeLocal.state()).thenReturn(ODHTNode.NodeState.PRODUCTION);
    when(nodeLocal.getNodeAddress()).thenReturn(nodeAddressStub);

    final ORID startRecordId = new ORecordId(1, new OClusterPositionNodeId(idToStart));

    final Iterator<ORecordMetadata> metadataIterator = mock(Iterator.class);

    when(nodeLocal.getLocalRingIterator(startRecordId.nextRid(), startRecordId)).thenReturn(metadataIterator);
    when(metadataIterator.hasNext()).thenReturn(false);

    final ONodeId result = globalMaintenanceProtocol.reallocateWrongPlacedReplicas(nodeLocal, idToStart, 1, 1);

    Assert.assertEquals(result, nodeId);

    verify(nodeLocal).state();
    verify(nodeLocal).getNodeAddress();
    verify(nodeLocal).getLocalRingIterator(startRecordId.nextRid(), startRecordId);

    verifyZeroInteractions(replicaDistributionStrategy, nodeLookup);
    verifyNoMoreInteractions(nodeLocal);
  }

  public void testReallocateWrongPlacedReplicasStateOnlyIdToStartInDB() {
    final ONodeId idToStart = ONodeId.generateUniqueId();
    final ONodeId nodeId = ONodeId.generateUniqueId();

    final ONodeAddressStub nodeAddressStub = new ONodeAddressStub(nodeId);

    when(nodeLocal.state()).thenReturn(ODHTNode.NodeState.PRODUCTION);
    when(nodeLocal.getNodeAddress()).thenReturn(nodeAddressStub);

    final ORID startRecordId = new ORecordId(1, new OClusterPositionNodeId(idToStart));

    final Iterator<ORecordMetadata> metadataIterator = mock(Iterator.class);

    when(nodeLocal.getLocalRingIterator(startRecordId.nextRid(), startRecordId)).thenReturn(metadataIterator);
    when(metadataIterator.hasNext()).thenReturn(true);
    when(metadataIterator.next()).thenReturn(new ORecordMetadata(startRecordId, new ODHTRecordVersion()));

    final ONodeId result = globalMaintenanceProtocol.reallocateWrongPlacedReplicas(nodeLocal, idToStart, 1, 1);

    Assert.assertEquals(result, nodeId);

    verify(nodeLocal).state();
    verify(nodeLocal).getNodeAddress();
    verify(nodeLocal).getLocalRingIterator(startRecordId.nextRid(), startRecordId);

    verifyZeroInteractions(replicaDistributionStrategy, nodeLookup);
    verifyNoMoreInteractions(nodeLocal);
  }

  public void testReallocateWrongPlacedReplicasStateNodeContainsOnlyItsOwnItems() {
    final ONodeId idToStart = ONodeId.generateUniqueId();
    final ONodeId nodeId = ONodeId.generateUniqueId();

    final ONodeAddressStub nodeAddressStub = new ONodeAddressStub(nodeId);

    when(nodeLocal.state()).thenReturn(ODHTNode.NodeState.PRODUCTION);
    when(nodeLocal.getNodeAddress()).thenReturn(nodeAddressStub);

    final ORID startRecordId = new ORecordId(1, new OClusterPositionNodeId(idToStart));
    final ORID nextRecordId = startRecordId.nextRid();

    final Iterator<ORecordMetadata> metadataIterator = mock(Iterator.class);

    when(nodeLocal.getLocalRingIterator(startRecordId.nextRid(), startRecordId)).thenReturn(metadataIterator);
    when(metadataIterator.hasNext()).thenReturn(true);
    when(metadataIterator.next()).thenReturn(new ORecordMetadata(nextRecordId, new ODHTRecordVersion()));

    when(nodeLocal.findSuccessor(((OClusterPositionNodeId) nextRecordId.getClusterPosition()).getNodeId())).thenReturn(
        new ONodeAddressStub(nodeId));

    final ONodeId result = globalMaintenanceProtocol.reallocateWrongPlacedReplicas(nodeLocal, idToStart, 1, 1);

    Assert.assertEquals(result, nodeId);

    verify(nodeLocal).state();
    verify(nodeLocal).getNodeAddress();
    verify(nodeLocal).getLocalRingIterator(startRecordId.nextRid(), startRecordId);
    verify(nodeLocal).findSuccessor(((OClusterPositionNodeId) nextRecordId.getClusterPosition()).getNodeId());

    verifyZeroInteractions(replicaDistributionStrategy, nodeLookup);
    verifyNoMoreInteractions(nodeLocal);
  }

  public void testReallocateWrongPlacedReplicasStateNodeRecordsSuccessorIsOffline() {
    final ONodeId idToStart = ONodeId.generateUniqueId();
    final ONodeId nodeId = ONodeId.generateUniqueId();
    final ONodeId successorNodeId = ONodeId.generateUniqueId();

    final ONodeAddressStub nodeAddressStub = new ONodeAddressStub(nodeId);

    when(nodeLocal.state()).thenReturn(ODHTNode.NodeState.PRODUCTION);
    when(nodeLocal.getNodeAddress()).thenReturn(nodeAddressStub);

    final ORID startRecordId = new ORecordId(1, new OClusterPositionNodeId(idToStart));
    final ORID nextRecordId = startRecordId.nextRid();

    final Iterator<ORecordMetadata> metadataIterator = mock(Iterator.class);

    when(nodeLocal.getLocalRingIterator(startRecordId.nextRid(), startRecordId)).thenReturn(metadataIterator);
    when(metadataIterator.hasNext()).thenReturn(true);
    when(metadataIterator.next()).thenReturn(new ORecordMetadata(nextRecordId, new ODHTRecordVersion()));

    when(nodeLocal.findSuccessor(((OClusterPositionNodeId) nextRecordId.getClusterPosition()).getNodeId())).thenReturn(
        new ONodeAddressStub(successorNodeId));

    when(nodeLookup.findById(new ONodeAddressStub(successorNodeId))).thenReturn(null);

    final ONodeId result = globalMaintenanceProtocol.reallocateWrongPlacedReplicas(nodeLocal, idToStart, 1, 1);

    Assert.assertEquals(result, nodeId);

    verify(nodeLocal).state();
    verify(nodeLocal).getNodeAddress();
    verify(nodeLocal).getLocalRingIterator(startRecordId.nextRid(), startRecordId);
    verify(nodeLocal).findSuccessor(((OClusterPositionNodeId) nextRecordId.getClusterPosition()).getNodeId());
    verify(nodeLookup).findById(new ONodeAddressStub(successorNodeId));

    verifyZeroInteractions(replicaDistributionStrategy);
    verifyNoMoreInteractions(nodeLocal);
    verifyNoMoreInteractions(nodeLookup);
  }

  public void testReallocateWrongPlacedReplicasStateNodeRecordsNodeIsSyncReplicaHolder() {
    final ONodeId idToStart = ONodeId.generateUniqueId();
    final ONodeId nodeId = ONodeId.generateUniqueId();
    final ONodeId successorNodeId = ONodeId.generateUniqueId();

    final ONodeAddressStub nodeAddressStub = new ONodeAddressStub(nodeId);

    when(nodeLocal.state()).thenReturn(ODHTNode.NodeState.PRODUCTION);
    when(nodeLocal.getNodeAddress()).thenReturn(nodeAddressStub);

    final ORID startRecordId = new ORecordId(1, new OClusterPositionNodeId(idToStart));
    final ORID nextRecordId = startRecordId.nextRid();

    final Iterator<ORecordMetadata> metadataIterator = mock(Iterator.class);

    when(nodeLocal.getLocalRingIterator(startRecordId.nextRid(), startRecordId)).thenReturn(metadataIterator);
    when(metadataIterator.hasNext()).thenReturn(true);
    when(metadataIterator.next()).thenReturn(new ORecordMetadata(nextRecordId, new ODHTRecordVersion()));

    when(nodeLocal.findSuccessor(((OClusterPositionNodeId) nextRecordId.getClusterPosition()).getNodeId())).thenReturn(
        new ONodeAddressStub(successorNodeId));

    final ODHTNode successorNode = mock(ODHTNode.class);

    when(nodeLookup.findById(new ONodeAddressStub(successorNodeId))).thenReturn(successorNode);

    final ONodeAddress[] recordSuccessors = new ONodeAddress[] { new ONodeAddressStub(nodeId),
        new ONodeAddressStub(ONodeId.generateUniqueId()), new ONodeAddressStub(ONodeId.generateUniqueId()) };

    when(successorNode.getSuccessors()).thenReturn(recordSuccessors);
    when(replicaDistributionStrategy.chooseReplicas(recordSuccessors, 1, 1)).thenReturn(
        new Set[] { new HashSet<ONodeAddress>(Arrays.asList(recordSuccessors[0])),
            new HashSet<ONodeAddress>(Arrays.asList(recordSuccessors[1])) });

    final ONodeId result = globalMaintenanceProtocol.reallocateWrongPlacedReplicas(nodeLocal, idToStart, 1, 1);

    Assert.assertEquals(result, nodeId);

    verify(nodeLocal).state();
    verify(nodeLocal).getNodeAddress();
    verify(nodeLocal).getLocalRingIterator(startRecordId.nextRid(), startRecordId);
    verify(nodeLocal).findSuccessor(((OClusterPositionNodeId) nextRecordId.getClusterPosition()).getNodeId());
    verify(nodeLookup).findById(new ONodeAddressStub(successorNodeId));

    verifyZeroInteractions(nodeLocal);
    verifyNoMoreInteractions(nodeLocal);
    verifyNoMoreInteractions(nodeLookup);
  }

  public void testReallocateWrongPlacedReplicasStateNodeRecordsNodeIsAsyncReplicaHolder() {
    final ONodeId idToStart = ONodeId.generateUniqueId();
    final ONodeId nodeId = ONodeId.generateUniqueId();
    final ONodeId successorNodeId = ONodeId.generateUniqueId();

    final ONodeAddressStub nodeAddressStub = new ONodeAddressStub(nodeId);

    when(nodeLocal.state()).thenReturn(ODHTNode.NodeState.PRODUCTION);
    when(nodeLocal.getNodeAddress()).thenReturn(nodeAddressStub);

    final ORID startRecordId = new ORecordId(1, new OClusterPositionNodeId(idToStart));
    final ORID nextRecordId = startRecordId.nextRid();

    final Iterator<ORecordMetadata> metadataIterator = mock(Iterator.class);

    when(nodeLocal.getLocalRingIterator(startRecordId.nextRid(), startRecordId)).thenReturn(metadataIterator);
    when(metadataIterator.hasNext()).thenReturn(true);
    when(metadataIterator.next()).thenReturn(new ORecordMetadata(nextRecordId, new ODHTRecordVersion()));

    when(nodeLocal.findSuccessor(((OClusterPositionNodeId) nextRecordId.getClusterPosition()).getNodeId())).thenReturn(
        new ONodeAddressStub(successorNodeId));

    final ODHTNode successorNode = mock(ODHTNode.class);

    when(nodeLookup.findById(new ONodeAddressStub(successorNodeId))).thenReturn(successorNode);

    final ONodeAddress[] recordSuccessors = new ONodeAddress[] { new ONodeAddressStub(nodeId),
        new ONodeAddressStub(ONodeId.generateUniqueId()), new ONodeAddressStub(ONodeId.generateUniqueId()) };

    when(successorNode.getSuccessors()).thenReturn(recordSuccessors);
    when(replicaDistributionStrategy.chooseReplicas(recordSuccessors, 1, 1)).thenReturn(
        new Set[] { new HashSet<ONodeAddress>(Arrays.asList(recordSuccessors[1])),
            new HashSet<ONodeAddress>(Arrays.asList(recordSuccessors[0])) });

    final ONodeId result = globalMaintenanceProtocol.reallocateWrongPlacedReplicas(nodeLocal, idToStart, 1, 1);

    Assert.assertEquals(result, nodeId);

    verify(nodeLocal).state();
    verify(nodeLocal).getNodeAddress();
    verify(nodeLocal).getLocalRingIterator(startRecordId.nextRid(), startRecordId);
    verify(nodeLocal).findSuccessor(((OClusterPositionNodeId) nextRecordId.getClusterPosition()).getNodeId());
    verify(nodeLookup).findById(new ONodeAddressStub(successorNodeId));

    verifyZeroInteractions(nodeLocal);
    verifyNoMoreInteractions(nodeLocal);
    verifyNoMoreInteractions(nodeLookup);
  }

  public void testReallocateWrongPlacedReplicasStateNodeRecordsTenRecordsIsNeededToBeRelocated() {
    final ONodeId idToStart = ONodeId.generateUniqueId();
    final ONodeId nodeId = ONodeId.generateUniqueId();
    final ONodeId successorNodeId = ONodeId.generateUniqueId();

    final ONodeAddressStub nodeAddressStub = new ONodeAddressStub(nodeId);

    when(nodeLocal.state()).thenReturn(ODHTNode.NodeState.PRODUCTION);
    when(nodeLocal.getNodeAddress()).thenReturn(nodeAddressStub);

    final ORID startRecordId = new ORecordId(1, new OClusterPositionNodeId(idToStart));
    final ORID nextRecordId = startRecordId.nextRid();

    final Iterator<ORecordMetadata> metadataIterator = mock(Iterator.class);

    when(nodeLocal.getLocalRingIterator(startRecordId.nextRid(), startRecordId)).thenReturn(metadataIterator);
    when(metadataIterator.hasNext()).thenReturn(true);
    when(metadataIterator.next()).thenReturn(new ORecordMetadata(nextRecordId, new ODHTRecordVersion()));

    when(nodeLocal.findSuccessor(((OClusterPositionNodeId) nextRecordId.getClusterPosition()).getNodeId())).thenReturn(
        new ONodeAddressStub(successorNodeId));

    final ODHTNode successorNode = mock(ODHTNode.class);

    when(nodeLookup.findById(new ONodeAddressStub(successorNodeId))).thenReturn(successorNode);

    final ONodeAddress[] recordSuccessors = new ONodeAddress[] { new ONodeAddressStub(ONodeId.generateUniqueId()),
        new ONodeAddressStub(ONodeId.generateUniqueId()), new ONodeAddressStub(ONodeId.generateUniqueId()) };

    when(successorNode.getSuccessors()).thenReturn(recordSuccessors);
    when(replicaDistributionStrategy.chooseReplicas(recordSuccessors, 2, 1)).thenReturn(
        new Set[] { new HashSet<ONodeAddress>(Arrays.asList(recordSuccessors[0])),
            new HashSet<ONodeAddress>(Arrays.asList(recordSuccessors[1])) });

    final ArrayList<Record> missedRecords = new ArrayList<Record>();
    final List<ORecordMetadata> missedMetadata = new ArrayList<ORecordMetadata>();

    for (int i = 0; i < 10; i++) {
      final ORID missedRid = new ORecordId(1, new OClusterPositionNodeId(ONodeId.generateUniqueId()));
      final Record missedRecord = new Record(missedRid, "data");

      missedRecords.add(missedRecord);
      missedMetadata.add(new ORecordMetadata(missedRid, missedRecord.getVersion()));
    }

    when(nodeLocal.getLocalRingIterator(startRecordId, new ORecordId(1, new OClusterPositionNodeId(successorNodeId)))).thenReturn(
        missedMetadata.iterator());

    final ODHTNode firstReplicaHolder = mock(ODHTNode.class);
    final ODHTNode secondReplicaHolder = mock(ODHTNode.class);

    when(nodeLookup.findById(recordSuccessors[0])).thenReturn(firstReplicaHolder);
    when(nodeLookup.findById(recordSuccessors[1])).thenReturn(secondReplicaHolder);

    final ORID[] missedIDs = new ORID[10];
    for (int i = 0; i < 10; i++)
      missedIDs[i] = missedMetadata.get(i).getId();

    when(successorNode.findMissedRecords(missedMetadata.toArray(new ORecordMetadata[0]))).thenReturn(missedIDs);
    when(firstReplicaHolder.findMissedRecords(missedMetadata.toArray(new ORecordMetadata[0]))).thenReturn(missedIDs);
    when(secondReplicaHolder.findMissedRecords(missedMetadata.toArray(new ORecordMetadata[0]))).thenReturn(missedIDs);

    for (Record missedRecord : missedRecords)
      when(nodeLocal.readRecordLocal(missedRecord.getId())).thenReturn(missedRecord);

    final ONodeId result = globalMaintenanceProtocol.reallocateWrongPlacedReplicas(nodeLocal, idToStart, 2, 1);

    Assert.assertEquals(result, successorNodeId);

    for (Record record : missedRecords) {
      verify(successorNode).updateReplica(record, false);
      verify(firstReplicaHolder).updateReplica(record, false);
      verify(secondReplicaHolder).updateReplica(record, false);

      verify(nodeLocal).cleanOutData(record.getId(), record.getVersion());
    }
  }

  public void testReallocateWrongPlacedReplicasStateNodeRecords64RecordsIsNeededToBeRelocated() {
    final ONodeId idToStart = ONodeId.generateUniqueId();
    final ONodeId nodeId = ONodeId.generateUniqueId();
    final ONodeId successorNodeId = ONodeId.generateUniqueId();

    final ONodeAddressStub nodeAddressStub = new ONodeAddressStub(nodeId);

    when(nodeLocal.state()).thenReturn(ODHTNode.NodeState.PRODUCTION);
    when(nodeLocal.getNodeAddress()).thenReturn(nodeAddressStub);

    final ORID startRecordId = new ORecordId(1, new OClusterPositionNodeId(idToStart));
    final ORID nextRecordId = startRecordId.nextRid();

    final Iterator<ORecordMetadata> metadataIterator = mock(Iterator.class);

    when(nodeLocal.getLocalRingIterator(startRecordId.nextRid(), startRecordId)).thenReturn(metadataIterator);
    when(metadataIterator.hasNext()).thenReturn(true);
    when(metadataIterator.next()).thenReturn(new ORecordMetadata(nextRecordId, new ODHTRecordVersion()));

    when(nodeLocal.findSuccessor(((OClusterPositionNodeId) nextRecordId.getClusterPosition()).getNodeId())).thenReturn(
        new ONodeAddressStub(successorNodeId));

    final ODHTNode successorNode = mock(ODHTNode.class);

    when(nodeLookup.findById(new ONodeAddressStub(successorNodeId))).thenReturn(successorNode);

    final ONodeAddress[] recordSuccessors = new ONodeAddress[] { new ONodeAddressStub(ONodeId.generateUniqueId()),
        new ONodeAddressStub(ONodeId.generateUniqueId()), new ONodeAddressStub(ONodeId.generateUniqueId()) };

    when(successorNode.getSuccessors()).thenReturn(recordSuccessors);
    when(replicaDistributionStrategy.chooseReplicas(recordSuccessors, 2, 1)).thenReturn(
        new Set[] { new HashSet<ONodeAddress>(Arrays.asList(recordSuccessors[0])),
            new HashSet<ONodeAddress>(Arrays.asList(recordSuccessors[1])) });

    final ArrayList<Record> missedRecords = new ArrayList<Record>();
    final List<ORecordMetadata> missedMetadata = new ArrayList<ORecordMetadata>();

    for (int i = 0; i < 64; i++) {
      final ORID missedRid = new ORecordId(1, new OClusterPositionNodeId(ONodeId.generateUniqueId()));
      final Record missedRecord = new Record(missedRid, "data");

      missedRecords.add(missedRecord);
      missedMetadata.add(new ORecordMetadata(missedRid, missedRecord.getVersion()));
    }

    when(nodeLocal.getLocalRingIterator(startRecordId, new ORecordId(1, new OClusterPositionNodeId(successorNodeId)))).thenReturn(
        missedMetadata.iterator());

    final ODHTNode firstReplicaHolder = mock(ODHTNode.class);
    final ODHTNode secondReplicaHolder = mock(ODHTNode.class);

    when(nodeLookup.findById(recordSuccessors[0])).thenReturn(firstReplicaHolder);
    when(nodeLookup.findById(recordSuccessors[1])).thenReturn(secondReplicaHolder);

    final ORID[] missedIDs = new ORID[64];
    for (int i = 0; i < 64; i++)
      missedIDs[i] = missedMetadata.get(i).getId();

    when(successorNode.findMissedRecords(missedMetadata.toArray(new ORecordMetadata[0]))).thenReturn(missedIDs);
    when(firstReplicaHolder.findMissedRecords(missedMetadata.toArray(new ORecordMetadata[0]))).thenReturn(missedIDs);
    when(secondReplicaHolder.findMissedRecords(missedMetadata.toArray(new ORecordMetadata[0]))).thenReturn(missedIDs);

    for (Record missedRecord : missedRecords)
      when(nodeLocal.readRecordLocal(missedRecord.getId())).thenReturn(missedRecord);

    final ONodeId result = globalMaintenanceProtocol.reallocateWrongPlacedReplicas(nodeLocal, idToStart, 2, 1);

    Assert.assertEquals(result, successorNodeId);

    for (Record record : missedRecords) {
      verify(successorNode).updateReplica(record, false);
      verify(firstReplicaHolder).updateReplica(record, false);
      verify(secondReplicaHolder).updateReplica(record, false);

      verify(nodeLocal).cleanOutData(record.getId(), record.getVersion());
    }
  }

  public void testReallocateWrongPlacedReplicasStateNodeRecords66RecordsIsNeededToBeRelocated() {
    final ONodeId idToStart = ONodeId.generateUniqueId();
    final ONodeId nodeId = ONodeId.generateUniqueId();
    final ONodeId successorNodeId = ONodeId.generateUniqueId();

    final ONodeAddressStub nodeAddressStub = new ONodeAddressStub(nodeId);

    when(nodeLocal.state()).thenReturn(ODHTNode.NodeState.PRODUCTION);
    when(nodeLocal.getNodeAddress()).thenReturn(nodeAddressStub);

    final ORID startRecordId = new ORecordId(1, new OClusterPositionNodeId(idToStart));
    final ORID nextRecordId = startRecordId.nextRid();

    final Iterator<ORecordMetadata> metadataIterator = mock(Iterator.class);

    when(nodeLocal.getLocalRingIterator(startRecordId.nextRid(), startRecordId)).thenReturn(metadataIterator);
    when(metadataIterator.hasNext()).thenReturn(true);
    when(metadataIterator.next()).thenReturn(new ORecordMetadata(nextRecordId, new ODHTRecordVersion()));

    when(nodeLocal.findSuccessor(((OClusterPositionNodeId) nextRecordId.getClusterPosition()).getNodeId())).thenReturn(
        new ONodeAddressStub(successorNodeId));

    final ODHTNode successorNode = mock(ODHTNode.class);

    when(nodeLookup.findById(new ONodeAddressStub(successorNodeId))).thenReturn(successorNode);

    final ONodeAddress[] recordSuccessors = new ONodeAddress[] { new ONodeAddressStub(ONodeId.generateUniqueId()),
        new ONodeAddressStub(ONodeId.generateUniqueId()), new ONodeAddressStub(ONodeId.generateUniqueId()) };

    when(successorNode.getSuccessors()).thenReturn(recordSuccessors);
    when(replicaDistributionStrategy.chooseReplicas(recordSuccessors, 2, 1)).thenReturn(
        new Set[] { new HashSet<ONodeAddress>(Arrays.asList(recordSuccessors[0])),
            new HashSet<ONodeAddress>(Arrays.asList(recordSuccessors[1])) });

    final ArrayList<Record> missedRecords = new ArrayList<Record>();
    final List<ORecordMetadata> missedMetadata = new ArrayList<ORecordMetadata>();

    for (int i = 0; i < 66; i++) {
      final ORID missedRid = new ORecordId(1, new OClusterPositionNodeId(ONodeId.generateUniqueId()));
      final Record missedRecord = new Record(missedRid, "data");

      missedRecords.add(missedRecord);
      missedMetadata.add(new ORecordMetadata(missedRid, missedRecord.getVersion()));
    }

    when(nodeLocal.getLocalRingIterator(startRecordId, new ORecordId(1, new OClusterPositionNodeId(successorNodeId)))).thenReturn(
        missedMetadata.iterator());

    final ODHTNode firstReplicaHolder = mock(ODHTNode.class);
    final ODHTNode secondReplicaHolder = mock(ODHTNode.class);

    when(nodeLookup.findById(recordSuccessors[0])).thenReturn(firstReplicaHolder);
    when(nodeLookup.findById(recordSuccessors[1])).thenReturn(secondReplicaHolder);

    final ORID[] missedIDs = new ORID[66];
    for (int i = 0; i < missedIDs.length; i++)
      missedIDs[i] = missedMetadata.get(i).getId();

    when(successorNode.findMissedRecords(missedMetadata.subList(0, 64).toArray(new ORecordMetadata[0]))).thenReturn(
        Arrays.copyOf(missedIDs, 64));
    when(firstReplicaHolder.findMissedRecords(missedMetadata.subList(0, 64).toArray(new ORecordMetadata[0]))).thenReturn(
						Arrays.copyOf(missedIDs, 64));
    when(secondReplicaHolder.findMissedRecords(missedMetadata.subList(0, 64).toArray(new ORecordMetadata[0]))).thenReturn(
        Arrays.copyOf(missedIDs, 64));

    when(successorNode.findMissedRecords(missedMetadata.subList(64, 66).toArray(new ORecordMetadata[0]))).thenReturn(
        Arrays.copyOfRange(missedIDs, 64, 66));
    when(firstReplicaHolder.findMissedRecords(missedMetadata.subList(64, 66).toArray(new ORecordMetadata[0]))).thenReturn(
						Arrays.copyOfRange(missedIDs, 64, 66));
    when(secondReplicaHolder.findMissedRecords(missedMetadata.subList(64, 66).toArray(new ORecordMetadata[0]))).thenReturn(
        Arrays.copyOfRange(missedIDs, 64, 66));

    for (Record missedRecord : missedRecords)
      when(nodeLocal.readRecordLocal(missedRecord.getId())).thenReturn(missedRecord);

    final ONodeId result = globalMaintenanceProtocol.reallocateWrongPlacedReplicas(nodeLocal, idToStart, 2, 1);

    Assert.assertEquals(result, successorNodeId);

    for (Record record : missedRecords) {
      verify(successorNode).updateReplica(record, false);
      verify(firstReplicaHolder).updateReplica(record, false);
      verify(secondReplicaHolder).updateReplica(record, false);

      verify(nodeLocal).cleanOutData(record.getId(), record.getVersion());
    }
  }

  public void testReallocateWrongPlacedReplicasStateNodeRecordsTenRecordsOneNodeIsNotFound() {
    final ONodeId idToStart = ONodeId.generateUniqueId();
    final ONodeId nodeId = ONodeId.generateUniqueId();
    final ONodeId successorNodeId = ONodeId.generateUniqueId();

    final ONodeAddressStub nodeAddressStub = new ONodeAddressStub(nodeId);

    when(nodeLocal.state()).thenReturn(ODHTNode.NodeState.PRODUCTION);
    when(nodeLocal.getNodeAddress()).thenReturn(nodeAddressStub);

    final ORID startRecordId = new ORecordId(1, new OClusterPositionNodeId(idToStart));
    final ORID nextRecordId = startRecordId.nextRid();

    final Iterator<ORecordMetadata> metadataIterator = mock(Iterator.class);

    when(nodeLocal.getLocalRingIterator(startRecordId.nextRid(), startRecordId)).thenReturn(metadataIterator);
    when(metadataIterator.hasNext()).thenReturn(true);
    when(metadataIterator.next()).thenReturn(new ORecordMetadata(nextRecordId, new ODHTRecordVersion()));

    when(nodeLocal.findSuccessor(((OClusterPositionNodeId) nextRecordId.getClusterPosition()).getNodeId())).thenReturn(
        new ONodeAddressStub(successorNodeId));

    final ODHTNode successorNode = mock(ODHTNode.class);

    when(nodeLookup.findById(new ONodeAddressStub(successorNodeId))).thenReturn(successorNode);

    final ONodeAddress[] recordSuccessors = new ONodeAddress[] { new ONodeAddressStub(ONodeId.generateUniqueId()),
        new ONodeAddressStub(ONodeId.generateUniqueId()), new ONodeAddressStub(ONodeId.generateUniqueId()) };

    when(successorNode.getSuccessors()).thenReturn(recordSuccessors);
    when(replicaDistributionStrategy.chooseReplicas(recordSuccessors, 2, 1)).thenReturn(
        new Set[] { new HashSet<ONodeAddress>(Arrays.asList(recordSuccessors[0])),
            new HashSet<ONodeAddress>(Arrays.asList(recordSuccessors[1])) });

    final ArrayList<Record> missedRecords = new ArrayList<Record>();
    final List<ORecordMetadata> missedMetadata = new ArrayList<ORecordMetadata>();

    for (int i = 0; i < 10; i++) {
      final ORID missedRid = new ORecordId(1, new OClusterPositionNodeId(ONodeId.generateUniqueId()));
      final Record missedRecord = new Record(missedRid, "data");

      missedRecords.add(missedRecord);
      missedMetadata.add(new ORecordMetadata(missedRid, missedRecord.getVersion()));
    }

    when(nodeLocal.getLocalRingIterator(startRecordId, new ORecordId(1, new OClusterPositionNodeId(successorNodeId)))).thenReturn(
        missedMetadata.iterator());

    final ODHTNode firstReplicaHolder = mock(ODHTNode.class);
    final ODHTNode secondReplicaHolder = mock(ODHTNode.class);

    when(nodeLookup.findById(recordSuccessors[0])).thenReturn(null);
    when(nodeLookup.findById(recordSuccessors[1])).thenReturn(secondReplicaHolder);

    final ORID[] missedIDs = new ORID[10];
    for (int i = 0; i < 10; i++)
      missedIDs[i] = missedMetadata.get(i).getId();

    when(successorNode.findMissedRecords(missedMetadata.toArray(new ORecordMetadata[0]))).thenReturn(missedIDs);
    when(firstReplicaHolder.findMissedRecords(missedMetadata.toArray(new ORecordMetadata[0]))).thenReturn(missedIDs);
    when(secondReplicaHolder.findMissedRecords(missedMetadata.toArray(new ORecordMetadata[0]))).thenReturn(missedIDs);

    for (Record missedRecord : missedRecords)
      when(nodeLocal.readRecordLocal(missedRecord.getId())).thenReturn(missedRecord);

    final ONodeId result = globalMaintenanceProtocol.reallocateWrongPlacedReplicas(nodeLocal, idToStart, 2, 1);

    Assert.assertEquals(result, successorNodeId);

    for (Record record : missedRecords) {
      verify(successorNode).updateReplica(record, false);
      verify(secondReplicaHolder).updateReplica(record, false);

      verify(nodeLocal).cleanOutData(record.getId(), record.getVersion());
    }
  }

  public void testReallocateWrongPlacedReplicasStateNodeRecordsTenRecordsOneNodeIsOffline() {
    final ONodeId idToStart = ONodeId.generateUniqueId();
    final ONodeId nodeId = ONodeId.generateUniqueId();
    final ONodeId successorNodeId = ONodeId.generateUniqueId();

    final ONodeAddressStub nodeAddressStub = new ONodeAddressStub(nodeId);

    when(nodeLocal.state()).thenReturn(ODHTNode.NodeState.PRODUCTION);
    when(nodeLocal.getNodeAddress()).thenReturn(nodeAddressStub);

    final ORID startRecordId = new ORecordId(1, new OClusterPositionNodeId(idToStart));
    final ORID nextRecordId = startRecordId.nextRid();

    final Iterator<ORecordMetadata> metadataIterator = mock(Iterator.class);

    when(nodeLocal.getLocalRingIterator(startRecordId.nextRid(), startRecordId)).thenReturn(metadataIterator);
    when(metadataIterator.hasNext()).thenReturn(true);
    when(metadataIterator.next()).thenReturn(new ORecordMetadata(nextRecordId, new ODHTRecordVersion()));

    when(nodeLocal.findSuccessor(((OClusterPositionNodeId) nextRecordId.getClusterPosition()).getNodeId())).thenReturn(
        new ONodeAddressStub(successorNodeId));

    final ODHTNode successorNode = mock(ODHTNode.class);

    when(nodeLookup.findById(new ONodeAddressStub(successorNodeId))).thenReturn(successorNode);

    final ONodeAddress[] recordSuccessors = new ONodeAddress[] { new ONodeAddressStub(ONodeId.generateUniqueId()),
        new ONodeAddressStub(ONodeId.generateUniqueId()), new ONodeAddressStub(ONodeId.generateUniqueId()) };

    when(successorNode.getSuccessors()).thenReturn(recordSuccessors);
    when(replicaDistributionStrategy.chooseReplicas(recordSuccessors, 2, 1)).thenReturn(
        new Set[] { new HashSet<ONodeAddress>(Arrays.asList(recordSuccessors[0])),
            new HashSet<ONodeAddress>(Arrays.asList(recordSuccessors[1])) });

    final ArrayList<Record> missedRecords = new ArrayList<Record>();
    final List<ORecordMetadata> missedMetadata = new ArrayList<ORecordMetadata>();

    for (int i = 0; i < 10; i++) {
      final ORID missedRid = new ORecordId(1, new OClusterPositionNodeId(ONodeId.generateUniqueId()));
      final Record missedRecord = new Record(missedRid, "data");

      missedRecords.add(missedRecord);
      missedMetadata.add(new ORecordMetadata(missedRid, missedRecord.getVersion()));
    }

    when(nodeLocal.getLocalRingIterator(startRecordId, new ORecordId(1, new OClusterPositionNodeId(successorNodeId)))).thenReturn(
        missedMetadata.iterator());

    final ODHTNode firstReplicaHolder = mock(ODHTNode.class);
    final ODHTNode secondReplicaHolder = mock(ODHTNode.class);

    when(nodeLookup.findById(recordSuccessors[0])).thenReturn(firstReplicaHolder);
    when(nodeLookup.findById(recordSuccessors[1])).thenReturn(secondReplicaHolder);

    final ORID[] missedIDs = new ORID[10];
    for (int i = 0; i < 10; i++)
      missedIDs[i] = missedMetadata.get(i).getId();

    when(successorNode.findMissedRecords(missedMetadata.toArray(new ORecordMetadata[0]))).thenReturn(missedIDs);
    when(firstReplicaHolder.findMissedRecords(missedMetadata.toArray(new ORecordMetadata[0]))).thenThrow(
						new ONodeOfflineException("", null, recordSuccessors[0].getNodeId()));
    when(secondReplicaHolder.findMissedRecords(missedMetadata.toArray(new ORecordMetadata[0]))).thenReturn(missedIDs);

    for (Record missedRecord : missedRecords)
      when(nodeLocal.readRecordLocal(missedRecord.getId())).thenReturn(missedRecord);

    final ONodeId result = globalMaintenanceProtocol.reallocateWrongPlacedReplicas(nodeLocal, idToStart, 2, 1);

    Assert.assertEquals(result, successorNodeId);

    for (Record record : missedRecords) {
      verify(successorNode).updateReplica(record, false);
      verify(secondReplicaHolder).updateReplica(record, false);

      verify(nodeLocal).cleanOutData(record.getId(), record.getVersion());
    }
  }

  public void testReallocateWrongPlacedReplicasStateNodeRecordsTenRecordsTwoRecordsIsOutOfDate() {
    final ONodeId idToStart = ONodeId.generateUniqueId();
    final ONodeId nodeId = ONodeId.generateUniqueId();
    final ONodeId successorNodeId = ONodeId.generateUniqueId();

    final ONodeAddressStub nodeAddressStub = new ONodeAddressStub(nodeId);

    when(nodeLocal.state()).thenReturn(ODHTNode.NodeState.PRODUCTION);
    when(nodeLocal.getNodeAddress()).thenReturn(nodeAddressStub);

    final ORID startRecordId = new ORecordId(1, new OClusterPositionNodeId(idToStart));
    final ORID nextRecordId = startRecordId.nextRid();

    final Iterator<ORecordMetadata> metadataIterator = mock(Iterator.class);

    when(nodeLocal.getLocalRingIterator(startRecordId.nextRid(), startRecordId)).thenReturn(metadataIterator);
    when(metadataIterator.hasNext()).thenReturn(true);
    when(metadataIterator.next()).thenReturn(new ORecordMetadata(nextRecordId, new ODHTRecordVersion()));

    when(nodeLocal.findSuccessor(((OClusterPositionNodeId) nextRecordId.getClusterPosition()).getNodeId())).thenReturn(
        new ONodeAddressStub(successorNodeId));

    final ODHTNode successorNode = mock(ODHTNode.class);

    when(nodeLookup.findById(new ONodeAddressStub(successorNodeId))).thenReturn(successorNode);

    final ONodeAddress[] recordSuccessors = new ONodeAddress[] { new ONodeAddressStub(ONodeId.generateUniqueId()),
        new ONodeAddressStub(ONodeId.generateUniqueId()), new ONodeAddressStub(ONodeId.generateUniqueId()) };

    when(successorNode.getSuccessors()).thenReturn(recordSuccessors);
    when(replicaDistributionStrategy.chooseReplicas(recordSuccessors, 2, 1)).thenReturn(
        new Set[] { new HashSet<ONodeAddress>(Arrays.asList(recordSuccessors[0])),
            new HashSet<ONodeAddress>(Arrays.asList(recordSuccessors[1])) });

    final ArrayList<Record> missedRecords = new ArrayList<Record>();
    final List<ORecordMetadata> missedMetadata = new ArrayList<ORecordMetadata>();

    for (int i = 0; i < 10; i++) {
      final ORID missedRid = new ORecordId(1, new OClusterPositionNodeId(ONodeId.generateUniqueId()));
      final Record missedRecord = new Record(missedRid, "data");

      missedRecords.add(missedRecord);
      missedMetadata.add(new ORecordMetadata(missedRid, missedRecord.getVersion()));
    }

    when(nodeLocal.getLocalRingIterator(startRecordId, new ORecordId(1, new OClusterPositionNodeId(successorNodeId)))).thenReturn(
        missedMetadata.iterator());

    final ODHTNode firstReplicaHolder = mock(ODHTNode.class);
    final ODHTNode secondReplicaHolder = mock(ODHTNode.class);

    when(nodeLookup.findById(recordSuccessors[0])).thenReturn(firstReplicaHolder);
    when(nodeLookup.findById(recordSuccessors[1])).thenReturn(secondReplicaHolder);

    final ORID[] missedIDs = new ORID[10];
    for (int i = 0; i < 10; i++)
      missedIDs[i] = missedMetadata.get(i).getId();

    when(successorNode.findMissedRecords(missedMetadata.toArray(new ORecordMetadata[0]))).thenReturn(missedIDs);
    when(firstReplicaHolder.findMissedRecords(missedMetadata.toArray(new ORecordMetadata[0]))).thenReturn(missedIDs);
    when(secondReplicaHolder.findMissedRecords(missedMetadata.toArray(new ORecordMetadata[0]))).thenReturn(missedIDs);

    for (Record missedRecord : missedRecords)
      when(nodeLocal.readRecordLocal(missedRecord.getId())).thenReturn(missedRecord);

    final Record outOfDateRecordOne = missedRecords.get(2);
    final Record outOfDateRecordTwo = missedRecords.get(3);

    doThrow(new OConcurrentModificationException(outOfDateRecordOne.getId(), 0, outOfDateRecordOne.getShortVersion(), 0)).when(
						nodeLocal).cleanOutData(outOfDateRecordOne.getId(), outOfDateRecordOne.getVersion());
    doThrow(new OConcurrentModificationException(outOfDateRecordTwo.getId(), 0, outOfDateRecordTwo.getShortVersion(), 0)).when(
						nodeLocal).cleanOutData(outOfDateRecordTwo.getId(), outOfDateRecordTwo.getVersion());

    final ONodeId result = globalMaintenanceProtocol.reallocateWrongPlacedReplicas(nodeLocal, idToStart, 2, 1);

    Assert.assertEquals(result, successorNodeId);

    for (Record record : missedRecords) {
      verify(successorNode).updateReplica(record, false);
      verify(firstReplicaHolder).updateReplica(record, false);
      verify(secondReplicaHolder).updateReplica(record, false);

      verify(nodeLocal).cleanOutData(record.getId(), record.getVersion());
    }
  }

  public void testReallocateWrongPlacedReplicasStateNodeRecordsTwoRecordsAreAbsent() {
    final ONodeId idToStart = ONodeId.generateUniqueId();
    final ONodeId nodeId = ONodeId.generateUniqueId();
    final ONodeId successorNodeId = ONodeId.generateUniqueId();

    final ONodeAddressStub nodeAddressStub = new ONodeAddressStub(nodeId);

    when(nodeLocal.state()).thenReturn(ODHTNode.NodeState.PRODUCTION);
    when(nodeLocal.getNodeAddress()).thenReturn(nodeAddressStub);

    final ORID startRecordId = new ORecordId(1, new OClusterPositionNodeId(idToStart));
    final ORID nextRecordId = startRecordId.nextRid();

    final Iterator<ORecordMetadata> metadataIterator = mock(Iterator.class);

    when(nodeLocal.getLocalRingIterator(startRecordId.nextRid(), startRecordId)).thenReturn(metadataIterator);
    when(metadataIterator.hasNext()).thenReturn(true);
    when(metadataIterator.next()).thenReturn(new ORecordMetadata(nextRecordId, new ODHTRecordVersion()));

    when(nodeLocal.findSuccessor(((OClusterPositionNodeId) nextRecordId.getClusterPosition()).getNodeId())).thenReturn(
        new ONodeAddressStub(successorNodeId));

    final ODHTNode successorNode = mock(ODHTNode.class);

    when(nodeLookup.findById(new ONodeAddressStub(successorNodeId))).thenReturn(successorNode);

    final ONodeAddress[] recordSuccessors = new ONodeAddress[] { new ONodeAddressStub(ONodeId.generateUniqueId()),
        new ONodeAddressStub(ONodeId.generateUniqueId()), new ONodeAddressStub(ONodeId.generateUniqueId()) };

    when(successorNode.getSuccessors()).thenReturn(recordSuccessors);
    when(replicaDistributionStrategy.chooseReplicas(recordSuccessors, 2, 1)).thenReturn(
        new Set[] { new HashSet<ONodeAddress>(Arrays.asList(recordSuccessors[0])),
            new HashSet<ONodeAddress>(Arrays.asList(recordSuccessors[1])) });

    final ArrayList<Record> missedRecords = new ArrayList<Record>();
    final List<ORecordMetadata> missedMetadata = new ArrayList<ORecordMetadata>();

    for (int i = 0; i < 10; i++) {
      final ORID missedRid = new ORecordId(1, new OClusterPositionNodeId(ONodeId.generateUniqueId()));
      final Record missedRecord = new Record(missedRid, "data");

      missedRecords.add(missedRecord);
      missedMetadata.add(new ORecordMetadata(missedRid, missedRecord.getVersion()));
    }

    when(nodeLocal.getLocalRingIterator(startRecordId, new ORecordId(1, new OClusterPositionNodeId(successorNodeId)))).thenReturn(
        missedMetadata.iterator());

    final ODHTNode firstReplicaHolder = mock(ODHTNode.class);
    final ODHTNode secondReplicaHolder = mock(ODHTNode.class);

    when(nodeLookup.findById(recordSuccessors[0])).thenReturn(firstReplicaHolder);
    when(nodeLookup.findById(recordSuccessors[1])).thenReturn(secondReplicaHolder);

    final ORID[] missedIDs = new ORID[10];
    for (int i = 0; i < 10; i++)
      missedIDs[i] = missedMetadata.get(i).getId();

    when(successorNode.findMissedRecords(missedMetadata.toArray(new ORecordMetadata[0]))).thenReturn(missedIDs);
    when(firstReplicaHolder.findMissedRecords(missedMetadata.toArray(new ORecordMetadata[0]))).thenReturn(missedIDs);
    when(secondReplicaHolder.findMissedRecords(missedMetadata.toArray(new ORecordMetadata[0]))).thenReturn(missedIDs);

    for (int i = 0; i < 10; i++) {
      final Record missedRecord = missedRecords.get(i);

      if (i != 1 && i != 2)
        when(nodeLocal.readRecordLocal(missedRecord.getId())).thenReturn(missedRecord);
      else {
				when(nodeLocal.readRecordLocal(missedRecord.getId())).thenReturn(null);
				doThrow(new  ORecordNotFoundException("")).
								when(nodeLocal).cleanOutData(missedRecord.getId(), missedRecord.getVersion());
			}
    }

    final ONodeId result = globalMaintenanceProtocol.reallocateWrongPlacedReplicas(nodeLocal, idToStart, 2, 1);

    Assert.assertEquals(result, successorNodeId);

    for (int i = 0; i < 10; i++) {
			final Record record = missedRecords.get(i);

			if (i != 1 && i != 2) {
				verify(successorNode).updateReplica(record, false);
				verify(firstReplicaHolder).updateReplica(record, false);
				verify(secondReplicaHolder).updateReplica(record, false);
			}

      verify(nodeLocal).cleanOutData(record.getId(), record.getVersion());
    }
  }

  private final class ONodeAddressStub extends ONodeAddress {
    private ONodeAddressStub(ONodeId nodeId) {
      super(nodeId);
    }
  }

}
