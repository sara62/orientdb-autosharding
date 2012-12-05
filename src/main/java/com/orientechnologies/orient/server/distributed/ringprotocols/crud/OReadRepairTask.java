package com.orientechnologies.orient.server.distributed.ringprotocols.crud;

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.server.distributed.ODHTNodeLocal;
import com.orientechnologies.orient.server.distributed.ONodeAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author Andrey Lomakin
 * @since 22.11.12
 */
final class OReadRepairTask implements Runnable {
	private static final Logger logger = LoggerFactory.getLogger(OReadRepairTask.class);

	private final ORID recordId;
	private final Set<ONodeAddress> replicaHolders;
	private final ODHTNodeLocal localNode;
	private final ORecordMergeStrategy recordMergeStrategy;
	private final ORecordMergeExecutionContext executionContext;

	OReadRepairTask(ORID recordId, Set<ONodeAddress> replicaHolders,
									ODHTNodeLocal localNode, ORecordMergeStrategy recordMergeStrategy,
									ORecordMergeExecutionContext executionContext) {
		this.recordId = recordId;
		this.replicaHolders = replicaHolders;
		this.localNode = localNode;
		this.recordMergeStrategy = recordMergeStrategy;
		this.executionContext = executionContext;
	}

	@Override
	public void run() {
		try {
			recordMergeStrategy.mergeReplicaVersions(localNode, recordId, replicaHolders, executionContext);
		} catch (Exception e) {
			logger.error("Exception during read repair for record " + recordId, e);
		}
	}
}