package com.orientechnologies.orient.server.distributed;

import com.orientechnologies.orient.core.id.ONodeId;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.version.ORecordVersion;
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
  public ONodeAddress getPredecessor();
	public NodeState state();
	public ONodeAddress[] getSuccessors();
	public void requestStabilization();
  public ONodeAddress notifyParent(ONodeAddress nodeId);
  public boolean joinDHT(ONodeAddress nodeId);
  public ONodeAddress findSuccessor(ONodeId id);

  public ORecordInternal<?> createRecord(String storageName, ORecordInternal<?> record);
  public ORecordInternal<?> readRecord(String storageName, ORID id);
  public ORecordInternal<?> updateRecord(String storageName, ORecordInternal<?> record);
  public void deleteRecord(String storageName, ORID id, ORecordVersion version);
  public void updateReplica(String storageName, ORecordInternal<?> replica, boolean async);
  public void updateReplicas(String storageName, ORecordInternal<?>[] replicas, boolean async);

  public ORecordInternal<?> getRecordFromNode(String storageName, ORID id);
  public ORecordInternal<?>[] getRecordsFromNode(String storageName, ORID[] ids);

  public ORecordInternal<?> createRecordInNode(String storageName, ORecordInternal<?> recordInternal);
  public ORecordInternal<?> updateRecordInNode(String storageName, ORecordInternal<?> record);
  public void deleteRecordFromNode(String storageName, ORID id, ORecordVersion version);
  public ORecordInternal<?> readRecordFromNode(String storageName, ORID id);
  public ORecordMetadata getRecordMetadataFromNode(String storageName, ORID id);
  public ORecordMetadata[] getRecordsForIntervalFromNode(String storageName, ORID startId, ORID endId);

  public ODetachedMerkleTreeNode findMerkleTreeNode(String storageName, ODetachedMerkleTreeNode node);
	public ORID[] findMissedRecords(String storageName, ORecordMetadata[] recordMetadatas);
	public int size(String storageName, int clusterId);
}
