package com.orientechnologies.orient.server.distributed.util;

import com.orientechnologies.orient.server.distributed.ODHTNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Andrey Lomakin
 * @since 22.11.12
 */
public abstract class OWaitTillNodeJoin {
	public static void waitTillNodeJoin(ODHTNode node) {
		Logger logger = LoggerFactory.getLogger(OWaitTillNodeJoin.class);

		while (!ODHTNode.NodeState.PRODUCTION.equals(node.state())) {
			logger.info("Wait till node {} will be joined.", node.getNodeAddress());

			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				throw new IllegalStateException(e);
			}
		}
	}
}