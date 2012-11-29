package com.orientechnologies.orient.server.distributed;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import com.orientechnologies.orient.core.id.ONodeId;

/**
 * @author Andrey Lomakin
 * @since 09.11.12
 */
public abstract class ONodeAddress implements Externalizable, Comparable<ONodeAddress> {
  private ONodeId nodeId;

  public ONodeAddress() {
  }

  public ONodeAddress(ONodeId nodeId) {
    this.nodeId = nodeId;
  }

  public final ONodeId getNodeId() {
    return nodeId;
  }

  @Override
  public int compareTo(ONodeAddress other) {
    return nodeId.compareTo(other.nodeId);
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    out.writeObject(nodeId);
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    nodeId = (ONodeId) in.readObject();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    ONodeAddress that = (ONodeAddress) o;

    if (!nodeId.equals(that.nodeId))
      return false;

    return true;
  }

  @Override
  public int hashCode() {
    return nodeId.hashCode();
  }

  @Override
  public String toString() {
    return nodeId.toString();
  }
}
