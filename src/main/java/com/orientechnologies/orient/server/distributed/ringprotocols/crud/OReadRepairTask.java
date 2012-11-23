package com.orientechnologies.orient.server.distributed.ringprotocols.crud;

import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.server.distributed.ODHTNodeLocal;
import com.orientechnologies.orient.server.distributed.ONodeAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
* @author Andrey Lomakin
* @since 22.11.12
*/
final class OReadRepairTask implements Callable<Void> {
	private static final Logger logger = LoggerFactory.getLogger(OReadRepairTask.class);

	private static final ExecutorService readRepairService = new ThreadPoolExecutor(0, Runtime.getRuntime().availableProcessors() / 2,
					60L, TimeUnit.SECONDS,
					new ArrayBlockingQueue<Runnable>(256),
					Executors.defaultThreadFactory(),
					new ThreadPoolExecutor.CallerRunsPolicy());

	private final ORecordId recordId;
	private final Set<ONodeAddress> replicaHolders;
	private final ODHTNodeLocal localNode;
	private final ORecordMergeStrategy recordMergeStrategy;

	OReadRepairTask(ORecordId recordId, Set<ONodeAddress> replicaHolders,
									ODHTNodeLocal localNode, ORecordMergeStrategy recordMergeStrategy) {
		this.recordId = recordId;
		this.replicaHolders = replicaHolders;
		this.localNode = localNode;
		this.recordMergeStrategy = recordMergeStrategy;
	}

	@Override
	public Void call() throws Exception {
		try {
			recordMergeStrategy.mergeReplicaVersions(localNode, recordId, replicaHolders);
		} catch (Exception e) {
			logger.error("Exception during read repair for record " + recordId, e);
		}

		return null;
	}

	public void submit() {
		readRepairService.submit(this);
	}
}