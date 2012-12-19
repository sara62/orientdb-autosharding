package com.orientechnologies.orient.server.distributed;

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.version.ORecordVersion;
import com.orientechnologies.orient.server.distributed.merkletree.OMerkleTree;

import java.util.Iterator;

/**
 * @author Andrey Lomakin
 * @since 22.11.12
 */
public interface ODHTNodeLocal extends ODHTNode {
	public ORecordInternal<?> addRecordLocal(String storageName, ORecordInternal<?> recordInternal);
	public void updateRecordLocal(String storageName, ORecordInternal<?> recordInternal);
	public ORecordInternal<?> readRecordLocal(String storageName, ORID recordId);
	public void removeRecordLocal(String storageName, ORID id, ORecordVersion version);
	public void cleanOutRecord(String storageName, ORID id, ORecordVersion version);

	public OMerkleTree getLocalMerkleTree(String storageName, int clusterId);

	public Iterator<ORecordMetadata> getLocalRingIterator(String storageName, ORID startRid, ORID endId);
}
