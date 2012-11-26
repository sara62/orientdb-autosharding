package com.orientechnologies.orient.server.distributed;

import com.orientechnologies.orient.core.id.OClusterPosition;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.server.distributed.merkletree.OMerkleTree;

/**
 * @author Andrey Lomakin
 * @since 22.11.12
 */
public interface ODHTNodeLocal extends ODHTNode {
	public Record addRecordLocal(ORID id, String data);
	public void updateRecordLocal(ORID id, Record record);
	public Record readRecordLocal(ORID recordId);
	public void removeRecordLocal(ORID id, ODHTRecordVersion version);
	public void cleanOutData(ORID id, ODHTRecordVersion version);

	public OMerkleTree getLocalMerkleTree();

	public ODatabaseRingIterator getLocalRingIterator(ORID startRid, ORID endId);
	public ORID getHigherLocalId(ORID clusterPosition);
	public ORID getCeilingLocalId(ORID clusterPosition);
}
