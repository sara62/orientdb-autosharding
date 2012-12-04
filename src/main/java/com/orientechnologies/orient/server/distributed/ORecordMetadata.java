package com.orientechnologies.orient.server.distributed;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.version.ORecordVersion;

/**
 * @author Andrey Lomakin
 * @since 01.10.12
 */
public final class ORecordMetadata implements Externalizable {
  private ORID              id;
  private ORecordVersion version;

  public ORecordMetadata() {
  }

  public ORecordMetadata(ORID id, ORecordVersion version) {
    this.id = id;
    this.version = version;
  }

  public ORID getId() {
    return id;
  }

  public ORecordVersion getVersion() {
    return version;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    ORecordMetadata that = (ORecordMetadata) o;

    if (id != null ? !id.equals(that.id) : that.id != null)
      return false;
    if (version != null ? !version.equals(that.version) : that.version != null)
      return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = id != null ? id.hashCode() : 0;
    result = 31 * result + (version != null ? version.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return "RecordMetadata{" + "id=" + id + ", version=" + version + '}';
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    out.writeObject(id);
    out.writeObject(version);
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    id = (ORID) in.readObject();
    version = (ORecordVersion) in.readObject();
  }
}
