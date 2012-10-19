package com.orientechnologies.orient.server.distributed;

/**
 * @author Andrey Lomakin
 * @since 17.08.12
 */
public interface ODHTNode {
  public enum NodeState {
    JOIN, PRODUCTION
  }

  public long getNodeId();

  public long getSuccessor();

  public long[] getSuccessors();

  public Long getPredecessor();

  public void requestStabilization();

  public long notifyParent(long nodeId);

  public boolean joinDHT(long nodeId);

  public long findSuccessor(long id);

  public NodeState state();

  public Record createRecord(String data);

  public Record createRecord(long id, String data);

  public Record getRecord(long id);

  public void updateRecord(long id, Record record);

  public void deleteRecord(long id, ODHTRecordVersion version);

  public void updateReplica(Record replica, boolean async);

  public Record getRecordFromNode(long id, boolean replicate);

  public Record createRecordInNode(long id, String data);

  public void updateRecordInNode(long id, Record record);

  public void deleteRecordFromNode(long id, ODHTRecordVersion version);

  public RecordMetadata getRecordMetadataFromNode(long id);

  public RecordMetadata[] getNodeRecordsForInterval(long startId, long endId);

  public ODetachedMerkleTreeNode findMerkleTreeNode(ODetachedMerkleTreeNode node, long requestorId);

  public int size();

  public long[] findMissedRecords(long[] ids, ODHTRecordVersion[] versions);
}
