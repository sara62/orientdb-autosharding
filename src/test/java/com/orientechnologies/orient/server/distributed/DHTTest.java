package com.orientechnologies.orient.server.distributed;

import com.orientechnologies.orient.server.hazelcast.ServerInstance;
import junit.framework.Assert;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * @author Andrey Lomakin
 * @since 20.08.12
 */
@Test
public class DHTTest {
	public void addNode() {
		ServerInstance serverInstance = new ServerInstance();
		serverInstance.init();
		while (true) ;
	}

	public void addData() throws Exception {
		ServerInstance serverInstance = new ServerInstance();
		serverInstance.init();

		Thread.sleep(20000);

		final Random random = new Random();

		Map<Long, String> addedData = new HashMap<Long, String>();
		for (int i = 0; i < 10000; i++) {
			long key = random.nextLong();
			if (key < 0)
				key = -key;

			final ODHTNode node = serverInstance.findSuccessor(key);
			node.put(key, String.valueOf(i));

			addedData.put(key, String.valueOf(i));
		}

		for (Map.Entry<Long, String> entry : addedData.entrySet()) {
			final ODHTNode node = serverInstance.findSuccessor(entry.getKey());
			Assert.assertEquals(entry.getValue(), node.get(entry.getKey()));
		}

		System.out.println("[stat] Node sizes : ");

		ODHTNode startNode = serverInstance.findSuccessor(0);
		System.out.println("[stat] Node : " + startNode.getNodeId() + " size - " + startNode.size());

		ODHTNode node = serverInstance.findById(startNode.getSuccessor());
		while (node.getNodeId() != startNode.getNodeId()) {
			System.out.println("[stat] Node : " + node.getNodeId() + " size - " + node.size());

			node = serverInstance.findById(node.getSuccessor());
		}
	}
}