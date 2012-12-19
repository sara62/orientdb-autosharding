package com.orientechnologies.orient.server.distributed.merkletree;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;

import com.orientechnologies.orient.core.id.ONodeId;
import com.orientechnologies.orient.server.distributed.ORecordMetadata;

/**
 * @author Andrey Lomakin
 * @since 02.10.12
 */
public final class ODetachedMerkleTreeNode implements Externalizable {
  private byte[]           hash;
  private ONodeId          startId;
  private ONodeId          endId;
	private int              clusterId;

  private ORecordMetadata[] recordMetadata;
  private byte[][]         childrenHash;

  private int[]            path;

  public ODetachedMerkleTreeNode() {
  }

  public ODetachedMerkleTreeNode(byte[] hash, ONodeId startId, ONodeId endId, int clusterId,
																 ORecordMetadata[] recordMetadata,
																 byte[][] childrenHash, int[] path) {
    this.hash = hash;

    this.startId = startId;
    this.endId = endId;

		this.clusterId = clusterId;

    this.recordMetadata = recordMetadata;
    this.childrenHash = childrenHash;

    this.path = path;
  }

  public int getRecordsCount() {
    return recordMetadata.length;
  }

  public ORecordMetadata getRecordMetadata(int index) {
    return recordMetadata[index];
  }

  public byte[] getChildHash(int index) {
    return childrenHash[index];
  }

  public byte[] getHash() {
    return hash;
  }

  public ONodeId getStartId() {
    return startId;
  }

  public ONodeId getEndId() {
    return endId;
  }

  public int getLevel() {
    return path.length;
  }

  public int getChildIndex() {
    return path[path.length - 1];
  }

  public int[] getPath() {
    return path;
  }

  public boolean isLeaf() {
    return recordMetadata != null;
  }

	public int getClusterId() {
		return clusterId;
	}

	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder();
		sb.append("ODetachedMerkleTreeNode");
		sb.append("{hash=").append(hash == null ? "null" : "");
		for (int i = 0; hash != null && i < hash.length; ++i)
			sb.append(i == 0 ? "" : ", ").append(Arrays.toString(hash));
		sb.append(", startId=").append(startId);
		sb.append(", endId=").append(endId);
		sb.append(", clusterId=").append(clusterId);
		sb.append(", recordMetadata=").append(recordMetadata == null ? "null" : Arrays.asList(recordMetadata).toString());
		sb.append(", childrenHash=").append(childrenHash == null ? "null" : Arrays.asList(childrenHash).toString());
		sb.append(", path=").append(path == null ? "null" : "");
		for (int i = 0; path != null && i < path.length; ++i)
			sb.append(i == 0 ? "" : ", ").append(Arrays.toString(path));
		sb.append('}');
		return sb.toString();
	}

	@Override
  public void writeExternal(ObjectOutput out) throws IOException {
		out.writeInt(clusterId);

    out.writeInt(hash.length);
    out.write(hash);

    out.writeObject(startId);
    out.writeObject(endId);

    out.writeInt(path.length);
    for (int pathItem : path)
      out.writeInt(pathItem);

    final boolean leaf = isLeaf();

    out.writeBoolean(leaf);

    if (leaf) {
      out.writeInt(recordMetadata.length);

      for (ORecordMetadata metadata : recordMetadata)
        out.writeObject(metadata);
    } else {
      out.writeInt(childrenHash.length);

      for (byte[] childHash : childrenHash) {
        out.writeInt(childHash.length);
        out.write(childHash);
      }
    }
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
		clusterId = in.readInt();

    int hashLength = in.readInt();
    hash = new byte[hashLength];

    int bytesWereRead = 0;
    while (bytesWereRead < hashLength) {
      int batchSize = in.read(hash, bytesWereRead, hashLength - bytesWereRead);
      if (batchSize == -1)
        break;

      bytesWereRead += batchSize;
    }

    startId = (ONodeId) in.readObject();
    endId = (ONodeId) in.readObject();

    final int pathLength = in.readInt();
    path = new int[pathLength];
    for (int i = 0; i < pathLength; i++)
      path[i] = in.readInt();

    boolean leaf = in.readBoolean();

    if (leaf) {
      final int metadataLength = in.readInt();

      recordMetadata = new ORecordMetadata[metadataLength];
      for (int i = 0; i < metadataLength; i++)
        recordMetadata[i] = (ORecordMetadata) in.readObject();
    } else {
      final int childrenHashLength = in.readInt();

      childrenHash = new byte[childrenHashLength][];
      for (int i = 0; i < childrenHashLength; i++) {
        hashLength = in.readInt();

        byte[] childHash = new byte[hashLength];

        bytesWereRead = 0;
        while (bytesWereRead < hashLength) {
          int batchSize = in.read(childHash, bytesWereRead, hashLength - bytesWereRead);
          if (batchSize == -1)
            break;

          bytesWereRead += batchSize;
        }

        childrenHash[i] = childHash;
      }
    }
  }
}
