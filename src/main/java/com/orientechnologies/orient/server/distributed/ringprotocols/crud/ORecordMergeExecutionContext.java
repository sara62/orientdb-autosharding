package com.orientechnologies.orient.server.distributed.ringprotocols.crud;

import com.orientechnologies.orient.server.distributed.ODHTNode;
import com.orientechnologies.orient.server.distributed.ONodeAddress;
import com.orientechnologies.orient.server.distributed.ORecordMetadata;

import java.util.HashSet;
import java.util.Set;

/**
 * @author Andrey Lomakin
 * @since 04.12.12
 */
public final class ORecordMergeExecutionContext {
	private final Set<ONodeAddress> replicaHoldersToUpdate = new HashSet<ONodeAddress>();
	private final Set<ONodeAddress> processedHolders = new HashSet<ONodeAddress>();

	private ODHTNode primaryHolder;
	private ORecordMetadata primaryMetadata;

	public Set<ONodeAddress> getReplicaHoldersToUpdate() {
		return replicaHoldersToUpdate;
	}

	public Set<ONodeAddress> getProcessedHolders() {
		return processedHolders;
	}

	public ODHTNode getPrimaryHolder() {
		return primaryHolder;
	}

	public ORecordMetadata getPrimaryMetadata() {
		return primaryMetadata;
	}

	public void setPrimaryHolder(ODHTNode primaryHolder) {
		this.primaryHolder = primaryHolder;
	}

	public void setPrimaryMetadata(ORecordMetadata primaryMetadata) {
		this.primaryMetadata = primaryMetadata;
	}
}