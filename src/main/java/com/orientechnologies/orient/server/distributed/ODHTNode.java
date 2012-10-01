package com.orientechnologies.orient.server.distributed;

/**
 * @author Andrey Lomakin
 * @since 17.08.12
 */
public interface ODHTNode {
  public enum NodeState {
    JOIN, MERGING, STABLE
  }

  public long getNodeId();

  public long getSuccessor();

  public long[] getSuccessors(int depth);

  public Long getPredecessor();

  public void requestStabilization();

  public long notifyParent(long nodeId);

  public boolean join(long nodeId);

  public long findSuccessor(long id);

  public NodeState state();

  public Record create(String data);
  public Record create(long id, String data);
  public Record get(long id);
	public void update(long id, Record record);
	public void remove(long id, int version);

	public void updateReplica(Record replica);

	public Record getRecordFromNode(long id);

  public int size();

  public long[] findMissedRecords(long[] ids, ODHTRecordVersion[] versions);
}
