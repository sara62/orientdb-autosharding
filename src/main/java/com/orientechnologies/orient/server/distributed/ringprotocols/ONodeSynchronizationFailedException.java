package com.orientechnologies.orient.server.distributed.ringprotocols;

/**
 * @author Andrey Lomakin
 * @since 23.11.12
 */
public final class ONodeSynchronizationFailedException extends RuntimeException {
  public ONodeSynchronizationFailedException(String message) {
    super(message);
  }
}
