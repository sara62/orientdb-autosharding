package com.orientechnologies.orient.server.distributed;

import com.orientechnologies.common.util.MersenneTwisterFast;
import com.orientechnologies.orient.server.hazelcast.ServerInstance;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Andrey Lomakin
 * @since 20.08.12
 */
@Test
public class DHTTest {
	private final AtomicBoolean testIsStopped = new AtomicBoolean(false);

	private ExecutorService readerExecutor = Executors.newCachedThreadPool(new ThreadFactory() {
		public Thread newThread(Runnable r) {
			final Thread thread = new Thread(r);
			thread.setDaemon(true);
			return thread;
		}
	});

	private ExecutorService writerExecutor = Executors.newCachedThreadPool(new ThreadFactory() {
		public Thread newThread(Runnable r) {
			final Thread thread = new Thread(r);
			thread.setDaemon(true);

			return thread;
		}
	});

	public void addNode() {
		ServerInstance serverInstance = new ServerInstance();
		serverInstance.init();
		while (true) ;
	}

	public void addData() throws Exception {
		final ServerInstance serverInstance = new ServerInstance();
		serverInstance.init();

		ServerInstance siNext = new ServerInstance();
		siNext.init();

		Thread.sleep(20000);

		final Map<Long, String> addedData = new ConcurrentHashMap<Long, String>();

		final List<Future<Void>> readerFutures = new ArrayList<Future<Void>>();

		List<Future<Void>> futures = new ArrayList<Future<Void>>();

		final int threadCount = 4;

		for (int i = 0; i < threadCount; i++)
			readerFutures.add(readerExecutor.submit(new DataReader(addedData, serverInstance)));


		final long interval = Long.MAX_VALUE / threadCount;

		for (long i = 0; i < threadCount; i++)
			futures.add(writerExecutor.submit(new DataWriter(i * interval, (i + 1) * interval,
							addedData, serverInstance, testIsStopped)));


		for (int i = 0; i < 5; i++) {
			ServerInstance si = new ServerInstance();
			si.init();

			Thread.sleep(5000);
		}

		Thread.sleep(10000);

		testIsStopped.set(true);

		for (Future<Void> future : futures)
			future.get();

		System.out.println("[stat] Items check " + addedData.size() + " items.");
		int i = 0;
		for (Map.Entry<Long, String> entry : addedData.entrySet()) {
			Assert.assertEquals(serverInstance.get(entry.getKey()), entry.getValue(), "Key " + entry.getKey() + " is absent");
			i++;
			if (i % 10000 == 0)
				System.out.println("[stat] " + i + " items were processed");
		}

		System.out.println("[stat] Node sizes : ");

		ODHTNode startNode = serverInstance.findSuccessor(0);
		System.out.println("[stat] Node : " + startNode.getNodeId() + " size - " + startNode.size());

		ODHTNode node = serverInstance.findById(startNode.getSuccessor());
		while (node.getNodeId() != startNode.getNodeId()) {
			System.out.println("[stat] Node : " + node.getNodeId() + " size - " + node.size());

			node = serverInstance.findById(node.getSuccessor());
		}

		for (Future<Void> future : readerFutures)
			future.get();
	}

	private static class DataWriter implements Callable<Void> {
		private final long start;
		private final long end;
		private final MersenneTwisterFast random = new MersenneTwisterFast();
		private final Map<Long, String> addedData;
		private final ServerInstance serverInstance;
		private final AtomicBoolean testIsStopped;

		private DataWriter(long start, long end, Map<Long, String> addedData,
											 ServerInstance serverInstance, AtomicBoolean testIsStopped) {
			this.start = start;
			this.end = end;
			this.addedData = addedData;
			this.serverInstance = serverInstance;
			this.testIsStopped = testIsStopped;
		}

		public Void call() throws Exception {
			while (!testIsStopped.get()) {
				long key = random.nextLong(end - start) + start;
				serverInstance.put(key, String.valueOf(key));

				addedData.put(key, String.valueOf(key));
			}
			return null;
		}
	}

	private class DataReader implements Callable<Void> {
		private final Map<Long, String> addedData;
		private final ServerInstance serverInstance;

		public DataReader(Map<Long, String> addedData, ServerInstance serverInstance) {
			this.addedData = addedData;
			this.serverInstance = serverInstance;
		}

		public Void call() throws Exception {
			while (!testIsStopped.get()) {
				for (Map.Entry<Long, String> entry : addedData.entrySet()) {
					Assert.assertEquals(serverInstance.get(entry.getKey()), entry.getValue(), "Key " + entry.getKey() + " is absent");
				}
			}
			return null;
		}
	}
}