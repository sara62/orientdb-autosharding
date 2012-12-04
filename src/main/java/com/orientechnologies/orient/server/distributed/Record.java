package com.orientechnologies.orient.server.distributed;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.version.ODistributedVersion;
import com.orientechnologies.orient.core.version.ORecordVersion;

/**
 * @author Andrey Lomakin
 * @since 19.09.12
 */
public class Record implements Externalizable {
  private String            data;
  private ORID              id;

  private ORecordVersion version;

  public Record() {
  }

  public Record(ORID id, String data) {
    this.id = id;
    this.data = data;

    version = new ODistributedVersion(0);
  }

  public Record(ORID id, String data, int shortVersion) {
    this.id = id;
    this.data = data;

    version = new ODistributedVersion(shortVersion);
  }

  public void updateData(String data, ORecordVersion version) {
    if (isTombstone())
      throw new IllegalStateException("Record was deleted and can not be updated.");

    if (this.version.compareTo(version) != 0)
      throw new IllegalStateException("Provided version is not up to date");

    this.version.increment();

    this.data = data;
  }

  public ORID getId() {
    return id;
  }

  public ORecordVersion getVersion() {
    return version;
  }

  public String getData() {
    return data;
  }

	public int getShortVersion() {
		return version.getCounter();
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

    if (data != null ? !data.equals(record.data) : record.data != null)
      return false;
    if (!id.equals(record.id))
      return false;
    if (!version.equals(record.version))
      return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = data != null ? data.hashCode() : 0;
    result = 31 * result + id.hashCode();
    result = 31 * result + version.hashCode();
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
    id = (ORID) in.readObject();
    version = (ORecordVersion) in.readObject();

    final boolean dataIsNotNull = in.readBoolean();

    if (dataIsNotNull)
      data = in.readUTF();
  }
}
