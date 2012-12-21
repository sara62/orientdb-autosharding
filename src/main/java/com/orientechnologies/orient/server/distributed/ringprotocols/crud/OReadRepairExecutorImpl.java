package com.orientechnologies.orient.server.distributed.ringprotocols.crud;

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.server.distributed.ODHTNodeLocal;
import com.orientechnologies.orient.server.distributed.ONodeAddress;

import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author Andrey Lomakin
 * @since 05.12.12
 */
public final class OReadRepairExecutorImpl implements OReadRepairExecutor {
	private static final ExecutorService readRepairService =
					new ThreadPoolExecutor(0, Runtime.getRuntime().availableProcessors() / 2,
									60, TimeUnit.SECONDS,
									new ArrayBlockingQueue<Runnable>(256),
									new OReadRepairFactory(),
									new ThreadPoolExecutor.CallerRunsPolicy());


	private final ORecordMergeStrategy recordMergeStrategy;

	public OReadRepairExecutorImpl(ORecordMergeStrategy recordMergeStrategy) {
		this.recordMergeStrategy = recordMergeStrategy;
	}

	@Override
	public void submit(String storageName, ORID recordId, Set<ONodeAddress> replicaHolders, ODHTNodeLocal localNode,
                     ORecordMergeExecutionContext executionContext) {
		readRepairService.submit(new OReadRepairTask(storageName, recordId, replicaHolders, localNode, recordMergeStrategy,
						executionContext));
	}

	private static final class OReadRepairFactory implements ThreadFactory {
		@Override
		public Thread newThread(Runnable r) {
			final Thread thread = new Thread(r);
			thread.setDaemon(true);

			return thread;
		}
	}

}