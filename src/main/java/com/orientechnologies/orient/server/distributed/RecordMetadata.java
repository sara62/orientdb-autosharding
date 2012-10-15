package com.orientechnologies.orient.server.distributed;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;


/**
 * @author Andrey Lomakin
 * @since 01.10.12
 */
public class RecordMetadata implements Externalizable {
  private long              id;
  private ODHTRecordVersion version;

  public RecordMetadata() {
  }

  public RecordMetadata(long id, ODHTRecordVersion version) {
    this.id = id;
    this.version = version;
  }

  public long getId() {
    return id;
  }

  public ODHTRecordVersion getVersion() {
    return version;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    RecordMetadata metadata = (RecordMetadata) o;

    if (id != metadata.id)
      return false;
    if (!version.equals(metadata.version))
      return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = (int) (id ^ (id >>> 32));
    result = 31 * result + version.hashCode();
    return result;
  }

  @Override
  public String toString() {
    return "RecordMetadata{" + "id=" + id + ", version=" + version + '}';
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    out.writeLong(id);
    out.writeObject(version);
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    id = in.readLong();
    version = (ODHTRecordVersion) in.readObject();
  }
}
