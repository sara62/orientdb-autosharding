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
public class ODetachedMerkleTreeNode implements Externalizable {
  private byte[]           hash;
  private ONodeId          startId;
  private ONodeId          endId;

  private ORecordMetadata[] recordMetadata;
  private byte[][]         childrenHash;

  private int[]            path;

  public ODetachedMerkleTreeNode() {
  }

  public ODetachedMerkleTreeNode(byte[] hash, ONodeId startId, ONodeId endId, ORecordMetadata[] recordMetadata,
      byte[][] childrenHash, int[] path) {
    this.hash = hash;

    this.startId = startId;
    this.endId = endId;

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

  @Override
  public String toString() {
    return "ODetachedMerkleTreeNode{" + "hash=" + Arrays.toString(hash) + ", startId=" + startId + ", endId=" + endId
        + ", recordMetadata=" + (recordMetadata == null ? null : Arrays.asList(recordMetadata)) + ", childrenHash="
        + (childrenHash == null ? null : Arrays.asList(childrenHash)) + ", path=" + Arrays.toString(path) + '}';
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
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
