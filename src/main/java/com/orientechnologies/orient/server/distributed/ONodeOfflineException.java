package com.orientechnologies.orient.server.distributed;

import com.orientechnologies.orient.core.id.ONodeId;
import com.orientechnologies.orient.server.distributed.ringprotocols.ORemoteNodeCallException;

/**
 * @author Andrey Lomakin
 * @since 03.09.12
 */
public class ONodeOfflineException extends ORemoteNodeCallException {
  public ONodeOfflineException(String message, Throwable cause, ONodeId nodeId) {
    super(message, cause, nodeId);
  }
}
