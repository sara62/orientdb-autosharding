package com.orientechnologies.orient.server.distributed;

/**
 * @author Andrey Lomakin
 * @since 17.08.12
 */
public interface ODHTNode {
	public long getNodeId();
	public long getSuccessor();
	public Long getPredecessor();
	public void notify(long node);
	public boolean join(long node);
	public long findSuccessor(long id);

	public void put(Long id, String data);
	public String get(Long id);
	public String get(Long id, boolean checkOwnerShip);

	public boolean remove(Long id);
	public int size();

	public void notifyMigrationEnd(long nodeId);
	public void requestMigration(long requesterId);
}
