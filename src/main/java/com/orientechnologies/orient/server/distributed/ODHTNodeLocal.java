package com.orientechnologies.orient.server.distributed;

import com.orientechnologies.orient.core.id.OClusterPosition;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.server.distributed.merkletree.OMerkleTree;

/**
 * @author Andrey Lomakin
 * @since 22.11.12
 */
public interface ODHTNodeLocal extends ODHTNode {
	public Record addRecordLocal(ORecordId id, String data);
	public void updateRecordLocal(ORecordId id, Record record);
	public Record readRecordLocal(ORecordId recordId);
	public void removeRecordLocal(ORecordId id, ODHTRecordVersion version);
	public void cleanOutData(ORecordId id, ODHTRecordVersion version);

	public OMerkleTree getLocalMerkleTree();

	public ODatabaseRingIterator getLocalRingIterator(ORecordId startRid, ORecordId endId);
	public ORecordId getHigherLocalId(ORecordId clusterPosition);
	public ORecordId getCeilingLocalId(ORecordId clusterPosition);
}
