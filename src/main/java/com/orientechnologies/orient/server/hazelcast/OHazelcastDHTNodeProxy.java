package com.orientechnologies.orient.server.hazelcast;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import com.hazelcast.core.DistributedTask;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;
import com.hazelcast.core.MemberLeftException;
import com.orientechnologies.orient.server.distributed.ODHTNode;
import com.orientechnologies.orient.server.distributed.ODHTRecordVersion;
import com.orientechnologies.orient.server.distributed.ODetachedMerkleTreeNode;
import com.orientechnologies.orient.server.distributed.ONodeOfflineException;
import com.orientechnologies.orient.server.distributed.ORemoteNodeCallException;
import com.orientechnologies.orient.server.distributed.Record;
import com.orientechnologies.orient.server.distributed.RecordMetadata;

/**
 * @author Andrey Lomakin
 * @since 17.08.12
 */
public class OHazelcastDHTNodeProxy implements ODHTNode {
  private final long              nodeId;
  private final Member            member;
  private final HazelcastInstance hazelcastInstance;

  public OHazelcastDHTNodeProxy(long nodeId, Member member, HazelcastInstance hazelcastInstance) {
    this.nodeId = nodeId;
    this.member = member;
    this.hazelcastInstance = hazelcastInstance;
  }

  public long getNodeId() {
    return nodeId;
  }

  public String getMemberUUID() {
    return member.getUuid();
  }

  public long getSuccessor() {
    return callOnRemoteMember(new GetSuccessorNodeCall(nodeId, member.getUuid()), false);
  }

  public Long getPredecessor() {
    return callOnRemoteMember(new GetPredecessorNodeCall(nodeId, member.getUuid()), false);
  }

  public long notifyParent(long parentId) {
    return callOnRemoteMember(new NotifyNodeCall(nodeId, member.getUuid(), parentId), false);
  }

  public boolean joinDHT(long joinNodeId) {
    return callOnRemoteMember(new JoinNodeCall(nodeId, member.getUuid(), joinNodeId), false);
  }

  public long findSuccessor(long id) {
    return callOnRemoteMember(new FindSuccessorNodeCall(nodeId, member.getUuid(), id), false);
  }

  public int size() {
    return callOnRemoteMember(new SizeNodeCall(nodeId, member.getUuid()), false);
  }

  public long[] findMissedRecords(RecordMetadata[] recordMetadatas) {
    return callOnRemoteMember(new FindMissedRecordsNodeCall(nodeId, member.getUuid(), recordMetadatas), false);
  }

  public NodeState state() {
    return callOnRemoteMember(new StateNodeCall(nodeId, member.getUuid()), false);
  }

  public long[] getSuccessors() {
    return callOnRemoteMember(new SuccessorsNodeCall(nodeId, member.getUuid()), false);
  }

  public void requestStabilization() {
    callOnRemoteMember(new RequestStabilizationNodeCall(nodeId, member.getUuid()), true);
  }

  @Override
  public Record createRecord(String data) {
    return callOnRemoteMember(new CreateNodeCall(nodeId, member.getUuid(), data), false);
  }

  @Override
  public Record createRecord(long id, String data) {
    return callOnRemoteMember(new CreateWithIdNodeCall(nodeId, member.getUuid(), id, data), false);
  }

  @Override
  public Record getRecord(long id) {
    return callOnRemoteMember(new GetNodeCall(nodeId, member.getUuid(), id), false);
  }

  @Override
  public void updateRecord(long id, Record record) {
    callOnRemoteMember(new UpdateNodeCall(nodeId, member.getUuid(), id, record), false);
  }

  @Override
  public void deleteRecord(long id, ODHTRecordVersion version) {
    callOnRemoteMember(new RemoveNodeCall(nodeId, member.getUuid(), id, version), false);
  }

  @Override
  public void updateReplica(Record replica, boolean async) {
    callOnRemoteMember(new UpdateReplicaNodeCall(nodeId, member.getUuid(), replica), async);
  }

  @Override
  public void updateReplicas(Record[] replicas, boolean async) {
    callOnRemoteMember(new UpdateReplicasNodeCall(nodeId, member.getUuid(), replicas), async);
  }

  @Override
  public Record getRecordFromNode(long id, boolean replicate) {
    return callOnRemoteMember(new GetRecordFromNodeNodeCall(nodeId, member.getUuid(), id, replicate), false);
  }

  @Override
  public Record[] getRecordsFromNode(long[] ids, boolean replicate) {
    return callOnRemoteMember(new GetRecordsFromNodeNodeCall(nodeId, member.getUuid(), ids, replicate), false);
  }

  @Override
  public RecordMetadata getRecordMetadataFromNode(long id) {
    return callOnRemoteMember(new GetRecordMetadataFromNodeNodeCall(nodeId, member.getUuid(), id), false);
  }

  @Override
  public RecordMetadata[] getRecordsForIntervalFromNode(long startId, long endId) {
    return callOnRemoteMember(new GetExistingRecordsForIntervalNodeCall(nodeId, member.getUuid(), startId, endId), false);
  }

  @Override
  public ODetachedMerkleTreeNode findMerkleTreeNode(ODetachedMerkleTreeNode node) {
    return callOnRemoteMember(new FindMerkleTreeNodeNodeCall(nodeId, member.getUuid(), node), false);
  }

  @Override
  public Record createRecordInNode(long id, String data) {
    return callOnRemoteMember(new CreateWithIdInNodeNodeCall(nodeId, member.getUuid(), id, data), false);
  }

  @Override
  public void updateRecordInNode(long id, Record record) {
    callOnRemoteMember(new UpdateRecordInNodeCall(nodeId, member.getUuid(), id, record), false);
  }

  @Override
  public void deleteRecordFromNode(long id, ODHTRecordVersion version) {
    callOnRemoteMember(new RemoveRecordFromNodeNodeCall(nodeId, member.getUuid(), id, version), false);
  }

  private <T> T callOnRemoteMember(final NodeCall<T> call, boolean async) {
    try {
      Future<T> future = (Future<T>) hazelcastInstance.getExecutorService().submit(new DistributedTask<T>(call, member));

      if (async)
        return null;

      return future.get();
    } catch (IllegalStateException ise) {
      if (!hazelcastInstance.getLifecycleService().isRunning())
        throw new ONodeOfflineException("Member with id " + nodeId + " was left.", ise, nodeId);
      throw ise;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new ORemoteNodeCallException("Error during remote call of node " + nodeId, e, nodeId);
    } catch (MemberLeftException mle) {
      throw new ONodeOfflineException("Member with id " + nodeId + " was left.", mle, nodeId);
    } catch (ExecutionException ee) {
      throw new ORemoteNodeCallException("Error during remote call of node " + nodeId, ee, nodeId);
    }
  }

  private static abstract class NodeCall<T> implements Callable<T>, Externalizable {
    protected long   nodeId;
    protected String memberUUID;

    public NodeCall() {
    }

    public NodeCall(long nodeId, String memberUUID) {
      this.nodeId = nodeId;
      this.memberUUID = memberUUID;
    }

    public T call() throws Exception {
      ServerInstance serverInstance = ServerInstance.INSTANCES.get(memberUUID);
      if (serverInstance == null)
        throw new ONodeOfflineException("Member with id " + nodeId + " was left.", null, nodeId);

      final ODHTNode node = serverInstance.findById(nodeId);
      if (node == null)
        throw new ONodeOfflineException("Member with id " + nodeId + " was left.", null, nodeId);

      return call(node);
    }

    protected abstract T call(ODHTNode node);

    public void writeExternal(ObjectOutput out) throws IOException {
      out.writeLong(nodeId);
      out.writeUTF(memberUUID);
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
      nodeId = in.readLong();
      memberUUID = in.readUTF();
    }
  }

  private static final class FindMissedRecordsNodeCall extends NodeCall<long[]> {
    private RecordMetadata[] recordMetadatas;

    public FindMissedRecordsNodeCall() {
    }

    private FindMissedRecordsNodeCall(long nodeId, String memberUUID, RecordMetadata[] recordMetadatas) {
      super(nodeId, memberUUID);
      this.recordMetadatas = recordMetadatas;
    }

    @Override
    protected long[] call(ODHTNode node) {
      return node.findMissedRecords(recordMetadatas);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
      super.writeExternal(out);

      out.writeInt(recordMetadatas.length);

      for (RecordMetadata recordMetadata : recordMetadatas)
        out.writeObject(recordMetadata);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
      super.readExternal(in);

      final int dataLength = in.readInt();
      recordMetadatas = new RecordMetadata[dataLength];

      for (int i = 0; i < dataLength; i++)
        recordMetadatas[i] = (RecordMetadata) in.readObject();
    }
  }

  private static final class UpdateReplicaNodeCall extends NodeCall<Void> {
    private Record record;

    public UpdateReplicaNodeCall() {
    }

    private UpdateReplicaNodeCall(long nodeId, String memberUUID, Record record) {
      super(nodeId, memberUUID);
      this.record = record;
    }

    @Override
    protected Void call(ODHTNode node) {
      node.updateReplica(record, false);

      return null;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
      super.writeExternal(out);
      out.writeObject(record);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
      super.readExternal(in);
      record = (Record) in.readObject();
    }
  }

  private static final class UpdateReplicasNodeCall extends NodeCall<Void> {
    private Record[] records;

    public UpdateReplicasNodeCall() {
    }

    private UpdateReplicasNodeCall(long nodeId, String memberUUID, Record[] records) {
      super(nodeId, memberUUID);
      this.records = records;
    }

    @Override
    protected Void call(ODHTNode node) {
      node.updateReplicas(records, false);

      return null;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
      super.writeExternal(out);
      out.writeInt(records.length);
      for (Record record : records)
        out.writeObject(record);

    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
      super.readExternal(in);

      int len = in.readInt();
      records = new Record[len];
      for (int i = 0; i < len; i++)
        records[i] = (Record) in.readObject();
    }
  }

  private static final class UpdateNodeCall extends NodeCall<Void> {
    private long   id;
    private Record record;

    public UpdateNodeCall() {
    }

    private UpdateNodeCall(long nodeId, String memberUUID, long id, Record record) {
      super(nodeId, memberUUID);
      this.id = id;
      this.record = record;
    }

    @Override
    protected Void call(ODHTNode node) {
      node.updateRecord(id, record);
      return null;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
      super.writeExternal(out);
      out.writeLong(id);
      out.writeObject(record);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
      super.readExternal(in);
      id = in.readLong();
      record = (Record) in.readObject();
    }
  }

  private static final class UpdateRecordInNodeCall extends NodeCall<Void> {
    private long   id;
    private Record record;

    public UpdateRecordInNodeCall() {
    }

    private UpdateRecordInNodeCall(long nodeId, String memberUUID, long id, Record record) {
      super(nodeId, memberUUID);
      this.id = id;
      this.record = record;
    }

    @Override
    protected Void call(ODHTNode node) {
      node.updateRecordInNode(id, record);
      return null;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
      super.writeExternal(out);
      out.writeLong(id);
      out.writeObject(record);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
      super.readExternal(in);
      id = in.readLong();
      record = (Record) in.readObject();
    }
  }

  private static final class CreateNodeCall extends NodeCall<Record> {
    private String data;

    public CreateNodeCall() {
    }

    private CreateNodeCall(long nodeId, String memberUUID, String data) {
      super(nodeId, memberUUID);
      this.data = data;
    }

    @Override
    protected Record call(ODHTNode node) {
      return node.createRecord(data);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
      super.writeExternal(out);
      out.writeUTF(data);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
      super.readExternal(in);
      data = in.readUTF();
    }
  }

  private static final class CreateWithIdNodeCall extends NodeCall<Record> {
    private String data;
    private long   id;

    public CreateWithIdNodeCall() {
    }

    private CreateWithIdNodeCall(long nodeId, String memberUUID, long id, String data) {
      super(nodeId, memberUUID);
      this.data = data;
      this.id = id;
    }

    @Override
    protected Record call(ODHTNode node) {
      return node.createRecord(id, data);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
      super.writeExternal(out);
      out.writeLong(id);
      out.writeUTF(data);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
      super.readExternal(in);
      id = in.readLong();
      data = in.readUTF();
    }
  }

  private static final class CreateWithIdInNodeNodeCall extends NodeCall<Record> {
    private String data;
    private long   id;

    public CreateWithIdInNodeNodeCall() {
    }

    private CreateWithIdInNodeNodeCall(long nodeId, String memberUUID, long id, String data) {
      super(nodeId, memberUUID);
      this.data = data;
      this.id = id;
    }

    @Override
    protected Record call(ODHTNode node) {
      return node.createRecordInNode(id, data);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
      super.writeExternal(out);
      out.writeLong(id);
      out.writeUTF(data);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
      super.readExternal(in);
      id = in.readLong();
      data = in.readUTF();
    }
  }

  private static final class GetSuccessorNodeCall extends NodeCall<Long> {
    public GetSuccessorNodeCall() {
    }

    private GetSuccessorNodeCall(long nodeId, String memberUUID) {
      super(nodeId, memberUUID);
    }

    @Override
    protected Long call(ODHTNode node) {
      return node.getSuccessor();
    }
  }

  private static final class SuccessorsNodeCall extends NodeCall<long[]> {
    public SuccessorsNodeCall() {
    }

    private SuccessorsNodeCall(long nodeId, String memberUUID) {
      super(nodeId, memberUUID);
    }

    @Override
    protected long[] call(ODHTNode node) {
      return node.getSuccessors();
    }
  }

  private static final class SizeNodeCall extends NodeCall<Integer> {
    public SizeNodeCall() {
    }

    private SizeNodeCall(long nodeId, String memberUUID) {
      super(nodeId, memberUUID);
    }

    @Override
    protected Integer call(ODHTNode node) {
      return node.size();
    }
  }

  private static final class StateNodeCall extends NodeCall<NodeState> {
    public StateNodeCall() {
    }

    private StateNodeCall(long nodeId, String memberUUID) {
      super(nodeId, memberUUID);
    }

    @Override
    protected NodeState call(ODHTNode node) {
      return node.state();
    }
  }

  private static final class GetPredecessorNodeCall extends NodeCall<Long> {
    public GetPredecessorNodeCall() {
    }

    private GetPredecessorNodeCall(long nodeId, String memberUUID) {
      super(nodeId, memberUUID);
    }

    @Override
    protected Long call(ODHTNode node) {
      return node.getPredecessor();
    }
  }

  private static final class JoinNodeCall extends NodeCall<Boolean> {
    private long joinNodeId;

    public JoinNodeCall() {
    }

    private JoinNodeCall(long nodeId, String memberUUID, long joinNodeId) {
      super(nodeId, memberUUID);
      this.joinNodeId = joinNodeId;
    }

    @Override
    protected Boolean call(ODHTNode node) {
      return node.joinDHT(joinNodeId);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
      super.writeExternal(out);
      out.writeLong(joinNodeId);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
      super.readExternal(in);
      joinNodeId = in.readLong();
    }
  }

  private static final class NotifyNodeCall extends NodeCall<Long> {
    private long notifyNodeId;

    public NotifyNodeCall() {
    }

    private NotifyNodeCall(long nodeId, String memberUUID, long notifyNodeId) {
      super(nodeId, memberUUID);
      this.notifyNodeId = notifyNodeId;
    }

    @Override
    protected Long call(ODHTNode node) {
      return node.notifyParent(notifyNodeId);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
      super.writeExternal(out);
      out.writeLong(notifyNodeId);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
      super.readExternal(in);
      notifyNodeId = in.readLong();
    }
  }

  private static final class FindMerkleTreeNodeNodeCall extends NodeCall<ODetachedMerkleTreeNode> {
    private ODetachedMerkleTreeNode merkleTreeNode;

    public FindMerkleTreeNodeNodeCall() {
    }

    private FindMerkleTreeNodeNodeCall(long nodeId, String memberUUID, ODetachedMerkleTreeNode merkleTreeNode) {
      super(nodeId, memberUUID);
      this.merkleTreeNode = merkleTreeNode;
    }

    @Override
    protected ODetachedMerkleTreeNode call(ODHTNode node) {
      return node.findMerkleTreeNode(merkleTreeNode);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
      super.writeExternal(out);
      out.writeObject(merkleTreeNode);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
      super.readExternal(in);
      merkleTreeNode = (ODetachedMerkleTreeNode) in.readObject();
    }
  }

  private static final class FindSuccessorNodeCall extends NodeCall<Long> {
    private long keyId;

    public FindSuccessorNodeCall() {
    }

    private FindSuccessorNodeCall(long nodeId, String memberUUID, long keyId) {
      super(nodeId, memberUUID);
      this.keyId = keyId;
    }

    @Override
    protected Long call(ODHTNode node) {
      return node.findSuccessor(keyId);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
      super.writeExternal(out);
      out.writeLong(keyId);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
      super.readExternal(in);
      keyId = in.readLong();
    }
  }

  private static final class GetNodeCall extends NodeCall<Record> {
    private long id;

    public GetNodeCall() {
    }

    private GetNodeCall(long nodeId, String memberUUID, long id) {
      super(nodeId, memberUUID);
      this.id = id;
    }

    @Override
    protected Record call(ODHTNode node) {
      return node.getRecord(id);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
      super.writeExternal(out);
      out.writeLong(id);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
      super.readExternal(in);
      id = in.readLong();
    }
  }

  private static final class GetExistingRecordsForIntervalNodeCall extends NodeCall<RecordMetadata[]> {
    private long startNodeId;
    private long endNodeId;

    public GetExistingRecordsForIntervalNodeCall() {
    }

    private GetExistingRecordsForIntervalNodeCall(long nodeId, String memberUUID, long startNodeId, long endNodeId) {
      super(nodeId, memberUUID);

      this.startNodeId = startNodeId;
      this.endNodeId = endNodeId;
    }

    @Override
    protected RecordMetadata[] call(ODHTNode node) {
      return node.getRecordsForIntervalFromNode(startNodeId, endNodeId);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
      super.writeExternal(out);

      out.writeLong(startNodeId);
      out.writeLong(endNodeId);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
      super.readExternal(in);

      startNodeId = in.readLong();
      endNodeId = in.readLong();
    }
  }

  private static final class GetRecordFromNodeNodeCall extends NodeCall<Record> {
    private long    id;
    private boolean replicate;

    public GetRecordFromNodeNodeCall() {
    }

    private GetRecordFromNodeNodeCall(long nodeId, String memberUUID, long id, boolean replicate) {
      super(nodeId, memberUUID);
      this.id = id;
      this.replicate = replicate;
    }

    @Override
    protected Record call(ODHTNode node) {
      return node.getRecordFromNode(id, replicate);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
      super.writeExternal(out);
      out.writeLong(id);
      out.writeBoolean(replicate);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
      super.readExternal(in);
      id = in.readLong();
      replicate = in.readBoolean();
    }
  }

  private static final class GetRecordsFromNodeNodeCall extends NodeCall<Record[]> {
    private long[]  ids;
    private boolean replicate;

    public GetRecordsFromNodeNodeCall() {
    }

    private GetRecordsFromNodeNodeCall(long nodeId, String memberUUID, long[] ids, boolean replicate) {
      super(nodeId, memberUUID);
      this.ids = ids;
      this.replicate = replicate;
    }

    @Override
    protected Record[] call(ODHTNode node) {
      return node.getRecordsFromNode(ids, replicate);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
      super.writeExternal(out);

      out.writeInt(ids.length);
      for (long id : ids)
        out.writeLong(id);

      out.writeBoolean(replicate);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
      super.readExternal(in);
      int len = in.readInt();
      ids = new long[len];

      for (int i = 0; i < len; i++)
        ids[i] = in.readLong();

      replicate = in.readBoolean();
    }
  }

  private static final class GetRecordMetadataFromNodeNodeCall extends NodeCall<RecordMetadata> {
    private long id;

    public GetRecordMetadataFromNodeNodeCall() {
    }

    private GetRecordMetadataFromNodeNodeCall(long nodeId, String memberUUID, long id) {
      super(nodeId, memberUUID);
      this.id = id;
    }

    @Override
    protected RecordMetadata call(ODHTNode node) {
      return node.getRecordMetadataFromNode(id);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
      super.writeExternal(out);
      out.writeLong(id);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
      super.readExternal(in);
      id = in.readLong();
    }
  }

  private static final class RemoveNodeCall extends NodeCall<Void> {
    private long              id;
    private ODHTRecordVersion version;

    public RemoveNodeCall() {
    }

    private RemoveNodeCall(long nodeId, String memberUUID, long id, ODHTRecordVersion version) {
      super(nodeId, memberUUID);
      this.id = id;
      this.version = version;
    }

    @Override
    protected Void call(ODHTNode node) {
      node.deleteRecord(id, version);
      return null;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
      super.writeExternal(out);
      out.writeLong(id);
      out.writeObject(version);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
      super.readExternal(in);
      id = in.readLong();
      version = (ODHTRecordVersion) in.readObject();
    }
  }

  private static final class RemoveRecordFromNodeNodeCall extends NodeCall<Void> {
    private long              id;
    private ODHTRecordVersion version;

    public RemoveRecordFromNodeNodeCall() {
    }

    private RemoveRecordFromNodeNodeCall(long nodeId, String memberUUID, long id, ODHTRecordVersion version) {
      super(nodeId, memberUUID);
      this.id = id;
      this.version = version;
    }

    @Override
    protected Void call(ODHTNode node) {
      node.deleteRecordFromNode(id, version);
      return null;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
      super.writeExternal(out);
      out.writeLong(id);
      out.writeObject(version);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
      super.readExternal(in);
      id = in.readLong();
      version = (ODHTRecordVersion) in.readObject();
    }
  }

  private static final class RequestStabilizationNodeCall extends NodeCall<Void> {
    public RequestStabilizationNodeCall() {
    }

    private RequestStabilizationNodeCall(long nodeId, String memberUUID) {
      super(nodeId, memberUUID);
    }

    @Override
    protected Void call(ODHTNode node) {
      node.requestStabilization();
      return null;
    }
  }
}
