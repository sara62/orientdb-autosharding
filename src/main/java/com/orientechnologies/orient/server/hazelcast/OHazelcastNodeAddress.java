package com.orientechnologies.orient.server.hazelcast;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import com.orientechnologies.orient.core.id.ONodeId;
import com.orientechnologies.orient.server.distributed.ONodeAddress;

/**
 * @author Andrey Lomakin
 * @since 09.11.12
 */
public class OHazelcastNodeAddress extends ONodeAddress {
  private String memberUUID;

  public OHazelcastNodeAddress() {
  }

  public OHazelcastNodeAddress(ONodeId nodeId, String memberUUID) {
    super(nodeId);
    this.memberUUID = memberUUID;
  }

  public String getMemberUUID() {
    return memberUUID;
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    super.writeExternal(out);
    out.writeUTF(memberUUID);
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    super.readExternal(in);
    memberUUID = in.readUTF();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    if (!super.equals(o))
      return false;

    OHazelcastNodeAddress that = (OHazelcastNodeAddress) o;

    if (!memberUUID.equals(that.memberUUID))
      return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + memberUUID.hashCode();
    return result;
  }

  @Override
  public String toString() {
    return "nodeAddress: 'memberUUID:" + memberUUID + ": nodeId:" + getNodeId() + "'";
  }
}
