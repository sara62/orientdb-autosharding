package com.orientechnologies.orient.server.distributed;

/**
 * @author Andrey Lomakin
 * @since 03.09.12
 */
public class ORemoteNodeCallException extends RuntimeException {
	private final long nodeId;

	public ORemoteNodeCallException(String message, Throwable cause, long nodeId) {
		super(message, cause);
		this.nodeId = nodeId;
	}

	public long getNodeId() {
		return nodeId;
	}
}