package com.orientechnologies.orient.server.distributed;

import com.orientechnologies.orient.core.id.ONodeId;

/**
 * @author Andrey Lomakin
 * @since 03.09.12
 */
public class ORemoteNodeCallException extends RuntimeException {
  private final ONodeId nodeId;

  public ORemoteNodeCallException(String message, Throwable cause, ONodeId nodeId) {
    super(message, cause);
    this.nodeId = nodeId;
  }

  public ONodeId getNodeId() {
    return nodeId;
  }
}
