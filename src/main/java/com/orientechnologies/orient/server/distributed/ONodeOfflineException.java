package com.orientechnologies.orient.server.distributed;

/**
 * @author Andrey Lomakin
 * @since 03.09.12
 */
public class ONodeOfflineException extends ORemoteNodeCallException {
  public ONodeOfflineException(String message, Throwable cause, ONodeId nodeId) {
    super(message, cause, nodeId);
  }
}
