package com.orientechnologies.orient.server.distributed;

import com.orientechnologies.orient.core.serialization.OMemoryStream;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * @author Andrey Lomakin
 * @since 19.09.12
 */
public final class ODHTRecordVersion implements Externalizable, Comparable<ODHTRecordVersion> {
	private static final byte[] CURRENT_MAC = getMac();


	private long version;
	private long ts;
	private byte[] mac;

	public ODHTRecordVersion() {
	}

	public void init() {
		version = 0;

		ts = System.nanoTime();
		mac = CURRENT_MAC;
	}

	public void init(long shortVersion) {
		version = shortVersion;

		ts = System.nanoTime();
		mac = CURRENT_MAC;
	}


	public void updateVersion() {
		if (isTombstone())
			throw new IllegalStateException("Record was deleted and can not be updated.");

		this.version++;

		ts = System.nanoTime();
		mac = CURRENT_MAC;
	}

	public long getShortVersion() {
		if (!isTombstone())
			return version;

		return version - Long.MIN_VALUE;
	}

	public void convertToTombstone() {
		if (isTombstone())
			throw new IllegalStateException("Record was deleted and can not be updated.");

		version = Long.MIN_VALUE + (version + 1);

		ts = System.nanoTime();
		mac = CURRENT_MAC;
	}

	public boolean isTombstone() {
		return version < 0;
	}

	public int compareTo(ODHTRecordVersion version) {
		final long versionOne = this.getShortVersion();
		final long versionTwo = version.getShortVersion();

		if (versionOne > versionTwo)
			return 1;
		if (versionOne < versionTwo)
			return -1;

		if (ts > version.ts)
			return 1;
		if (ts < version.ts)
			return -1;

		for (int i = 0; i < 6; i++) {
			if (mac[i] > version.mac[i])
				return 1;
			if (mac[i] < version.mac[i])
				return -1;
		}

		return 0;
	}

	public void writeExternal(ObjectOutput out) throws IOException {
		out.writeLong(version);
		out.writeLong(ts);

		out.write(mac);
	}

	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
		version = in.readLong();
		ts = in.readLong();

		mac = new byte[6];

		int length = 0;
		while (length < 6) {
			final int ln = in.read(mac, length, 6);

			if (ln == -1)
				break;

			length += ln;
		}
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		ODHTRecordVersion version1 = (ODHTRecordVersion) o;

		if (ts != version1.ts) return false;
		if (version != version1.version) return false;
		if (!Arrays.equals(mac, version1.mac)) return false;

		return true;
	}

	@Override
	public int hashCode() {
		int result = (int) (version ^ (version >>> 32));
		result = 31 * result + (int) (ts ^ (ts >>> 32));
		result = 31 * result + Arrays.hashCode(mac);
		return result;
	}


	public ByteBuffer toStream() {
		final ByteBuffer buffer = ByteBuffer.allocate(22);

		buffer.putLong(version);
		buffer.putLong(ts);
		buffer.put(mac);

		buffer.rewind();

		return buffer;
	}

	private static byte[] getMac() {
		try {
			InetAddress ip = InetAddress.getLocalHost();
			NetworkInterface networkInterface = NetworkInterface.getByInetAddress(ip);
			return networkInterface.getHardwareAddress();
		} catch (UnknownHostException e) {
			throw new IllegalStateException("Error during MAC address retrieval.", e);
		} catch (SocketException e) {
			throw new IllegalStateException("Error during MAC address retrieval.", e);
		}
	}
}