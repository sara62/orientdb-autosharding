package com.orientechnologies.orient.server.distributed;

/**
 * @author Andrey Lomakin
 * @since 17.08.12
 */
public interface ODHTNodeLookup {
  public ODHTNode findById(ONodeAddress address);

  public int size();
}
