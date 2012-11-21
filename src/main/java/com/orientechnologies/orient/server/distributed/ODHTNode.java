package com.orientechnologies.orient.server.distributed;

import com.orientechnologies.orient.core.id.ONodeId;
import com.orientechnologies.orient.core.id.ORecordId;

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

  public Record createRecord(ORecordId id, String data);

  public Record getRecord(ORecordId id);

  public void updateRecord(ORecordId id, Record record);

  public void deleteRecord(ORecordId id, ODHTRecordVersion version);

  public void updateReplica(Record replica, boolean async);

  public void updateReplicas(Record[] replicas, boolean async);

  public Record getRecordFromNode(ORecordId id, boolean replicate);

  public Record[] getRecordsFromNode(ORecordId[] ids, boolean replicate);

  public Record createRecordInNode(ORecordId id, String data);

  public void updateRecordInNode(ORecordId id, Record record);

  public void deleteRecordFromNode(ORecordId id, ODHTRecordVersion version);

  public RecordMetadata getRecordMetadataFromNode(ORecordId id);

  public RecordMetadata[] getRecordsForIntervalFromNode(ORecordId startId, ORecordId endId);

  public ODetachedMerkleTreeNode findMerkleTreeNode(ODetachedMerkleTreeNode node);

  public int size();

  public ORecordId[] findMissedRecords(RecordMetadata[] recordMetadatas);
}
