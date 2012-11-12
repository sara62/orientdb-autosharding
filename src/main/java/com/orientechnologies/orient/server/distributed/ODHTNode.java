package com.orientechnologies.orient.server.distributed;

import com.orientechnologies.orient.core.id.OAutoShardedRecordId;

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

  public Record createRecord(OAutoShardedRecordId id, String data);

  public Record getRecord(OAutoShardedRecordId id);

  public void updateRecord(OAutoShardedRecordId id, Record record);

  public void deleteRecord(OAutoShardedRecordId id, ODHTRecordVersion version);

  public void updateReplica(Record replica, boolean async);

  public void updateReplicas(Record[] replicas, boolean async);

  public Record getRecordFromNode(OAutoShardedRecordId id, boolean replicate);

  public Record[] getRecordsFromNode(OAutoShardedRecordId[] ids, boolean replicate);

  public Record createRecordInNode(OAutoShardedRecordId id, String data);

  public void updateRecordInNode(OAutoShardedRecordId id, Record record);

  public void deleteRecordFromNode(OAutoShardedRecordId id, ODHTRecordVersion version);

  public RecordMetadata getRecordMetadataFromNode(OAutoShardedRecordId id);

  public RecordMetadata[] getRecordsForIntervalFromNode(OAutoShardedRecordId startId, OAutoShardedRecordId endId);

  public ODetachedMerkleTreeNode findMerkleTreeNode(ODetachedMerkleTreeNode node);

  public int size();

  public OAutoShardedRecordId[] findMissedRecords(RecordMetadata[] recordMetadatas);
}
