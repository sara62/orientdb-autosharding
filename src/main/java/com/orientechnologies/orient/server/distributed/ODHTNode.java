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

	public long create(String data);
	public long create(long id, String data);

	public String get(long id);
	public String get(long id, boolean checkOwnerShip);

	public void update(long id, String data);
	public void update(long id, String data, boolean checkOwnerShip);

	public boolean remove(long id);
	public boolean remove(long id, boolean checkOwnerShip);

	public int size();

	public long[] missingRecords(long[] ids, ODHTRecordVersion[] versions);
}
