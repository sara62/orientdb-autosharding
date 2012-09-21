package com.orientechnologies.orient.server.distributed;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
/**
 * @author Andrey Lomakin
 * @since 19.09.12
 */
public class Record implements Externalizable {
	private String data;

	private ODHTRecordVersion version;

	public Record() {
	}

	public Record(String data) {
		this.data = data;

		version = new ODHTRecordVersion();
		version.init();
	}

	public Record(String data, long shortVersion) {
		this.data = data;

		version = new ODHTRecordVersion();
		version.init(shortVersion);
	}

	public void updateData(String data, int shortVersion) {
		if (isTombstone())
			throw new IllegalStateException("Record was deleted and can not be updated.");

		if (this.version.getShortVersion() != shortVersion)
			throw new IllegalStateException("Provided version is not up to date");

		this.version.updateVersion();

		this.data = data;
	}

	public ODHTRecordVersion getVersion() {
		return version;
	}

	public String getData() {
		return data;
	}

	public long getShortVersion() {
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
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		Record record = (Record) o;

		if (data != null ? !data.equals(record.data) : record.data != null) return false;
		if (version != null ? !version.equals(record.version) : record.version != null) return false;

		return true;
	}

	@Override
	public int hashCode() {
		int result = data != null ? data.hashCode() : 0;
		result = 31 * result + (version != null ? version.hashCode() : 0);
		return result;
	}

	public void writeExternal(ObjectOutput out) throws IOException {
		out.writeUTF(data);
		out.writeObject(version);
	}

	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
		data = in.readUTF();
		version = (ODHTRecordVersion)in.readObject();
	}
}