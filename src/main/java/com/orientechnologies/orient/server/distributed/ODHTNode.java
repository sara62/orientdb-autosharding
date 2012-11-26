package com.orientechnologies.orient.server.distributed;

import com.orientechnologies.orient.core.id.ONodeId;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.server.distributed.merkletree.ODetachedMerkleTreeNode;

/**
 * @author Andrey Lomakin
 * @since 17.08.12
 */
public interface ODHTNode {
  public enum NodeState {
    JOIN, PRODUCTION
  }

  public ONodeAddress getNodeAddress();

  public ONodeAddress getSuccessor();

  public ONodeAddress[] getSuccessors();

  public ONodeAddress getPredecessor();

  public void requestStabilization();

  public ONodeAddress notifyParent(ONodeAddress nodeId);

  public boolean joinDHT(ONodeAddress nodeId);

  public ONodeAddress findSuccessor(ONodeId id);

  public NodeState state();

  public Record createRecord(String data);

  public Record createRecord(ORID id, String data);

  public Record readRecord(ORID id);

  public void updateRecord(ORID id, Record record);

  public void deleteRecord(ORID id, ODHTRecordVersion version);

  public void updateReplica(Record replica, boolean async);

  public void updateReplicas(Record[] replicas, boolean async);

  public Record getRecordFromNode(ORID id);

  public Record[] getRecordsFromNode(ORID[] ids);

  public Record createRecordInNode(ORID id, String data);

  public void updateRecordInNode(ORID id, Record record);

  public void deleteRecordFromNode(ORID id, ODHTRecordVersion version);

  public Record readRecordFromNode(ORID id);

  public RecordMetadata getRecordMetadataFromNode(ORID id);

  public RecordMetadata[] getRecordsForIntervalFromNode(ORID startId, ORID endId);

  public ODetachedMerkleTreeNode findMerkleTreeNode(ODetachedMerkleTreeNode node);

  public int size();

  public ORID[] findMissedRecords(RecordMetadata[] recordMetadatas);
}
