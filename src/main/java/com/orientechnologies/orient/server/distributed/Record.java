package com.orientechnologies.orient.server.distributed;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import com.orientechnologies.orient.core.id.OAutoShardedRecordId;

/**
 * @author Andrey Lomakin
 * @since 19.09.12
 */
public class Record implements Externalizable {
  private String               data;
  private OAutoShardedRecordId id;

  private ODHTRecordVersion    version;

  public Record() {
  }

  public Record(OAutoShardedRecordId id, String data) {
    this.id = id;
    this.data = data;

    version = new ODHTRecordVersion();
    version.init();
  }

  public Record(OAutoShardedRecordId id, String data, int shortVersion) {
    this.id = id;
    this.data = data;

    version = new ODHTRecordVersion();
    version.init(shortVersion);
  }

  public void updateData(String data, ODHTRecordVersion version) {
    if (isTombstone())
      throw new IllegalStateException("Record was deleted and can not be updated.");

    if (this.version.compareTo(version) != 0)
      throw new IllegalStateException("Provided version is not up to date");

    this.version.updateVersion();

    this.data = data;
  }

  public OAutoShardedRecordId getId() {
    return id;
  }

  public ODHTRecordVersion getVersion() {
    return version;
  }

  public String getData() {
    return data;
  }

  public int getShortVersion() {
    return version.getShortVersion();
  }

  public int compareVersions(Record record) {
    return version.compareTo(record.version);
  }

  public boolean isTombstone() {
    return version.isTombstone();
  }

  public void convertToTombstone() {
    data = null;

    this.version.convertToTombstone();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    Record record = (Record) o;

    if (id != record.id)
      return false;
    if (!version.equals(record.version))
      return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = data != null ? data.hashCode() : 0;
    result = 31 * result + (id != null ? id.hashCode() : 0);
    result = 31 * result + (version != null ? version.hashCode() : 0);
    return result;
  }

  public void writeExternal(ObjectOutput out) throws IOException {
    out.writeObject(id);
    out.writeObject(version);

    final boolean dataIsNotNull = data != null;
    out.writeBoolean(dataIsNotNull);

    if (dataIsNotNull)
      out.writeUTF(data);
  }

  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    id = (OAutoShardedRecordId) in.readObject();
    version = (ODHTRecordVersion) in.readObject();

    final boolean dataIsNotNull = in.readBoolean();

    if (dataIsNotNull)
      data = in.readUTF();
  }
}
