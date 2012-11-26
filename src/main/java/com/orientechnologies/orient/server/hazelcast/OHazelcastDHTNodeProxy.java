package com.orientechnologies.orient.server.hazelcast;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import com.hazelcast.core.DistributedTask;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;
import com.hazelcast.core.MemberLeftException;
import com.orientechnologies.orient.core.id.ONodeId;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.server.distributed.ODHTNode;
import com.orientechnologies.orient.server.distributed.ODHTRecordVersion;
import com.orientechnologies.orient.server.distributed.ONodeAddress;
import com.orientechnologies.orient.server.distributed.ONodeOfflineException;
import com.orientechnologies.orient.server.distributed.Record;
import com.orientechnologies.orient.server.distributed.RecordMetadata;
import com.orientechnologies.orient.server.distributed.merkletree.ODetachedMerkleTreeNode;
import com.orientechnologies.orient.server.distributed.ringprotocols.ORemoteNodeCallException;

/**
 * @author Andrey Lomakin
 * @since 17.08.12
 */
public class OHazelcastDHTNodeProxy implements ODHTNode {
  private final OHazelcastNodeAddress nodeAddress;
  private final HazelcastInstance     hazelcastInstance;

  public OHazelcastDHTNodeProxy(OHazelcastNodeAddress nodeAddress, HazelcastInstance hazelcastInstance) {
    this.nodeAddress = nodeAddress;
    this.hazelcastInstance = hazelcastInstance;
  }

  @Override
  public ONodeAddress getNodeAddress() {
    return nodeAddress;
  }

  public String getMemberUUID() {
    return nodeAddress.getMemberUUID();
  }

  public ONodeAddress getSuccessor() {
    return callOnRemoteMember(new GetSuccessorNodeCall(nodeAddress), false);
  }

  public ONodeAddress getPredecessor() {
    return callOnRemoteMember(new GetPredecessorNodeCall(nodeAddress), false);
  }

  public ONodeAddress notifyParent(ONodeAddress parentAddress) {
    return callOnRemoteMember(new NotifyNodeCall(nodeAddress, parentAddress), false);
  }

  public boolean joinDHT(ONodeAddress joinNodeAddress) {
    return callOnRemoteMember(new JoinNodeCall(nodeAddress, joinNodeAddress), false);
  }

  public ONodeAddress findSuccessor(ONodeId id) {
    return callOnRemoteMember(new FindSuccessorNodeCall(nodeAddress, id), false);
  }

  public int size() {
    return callOnRemoteMember(new SizeNodeCall(nodeAddress), false);
  }

  public ORID[] findMissedRecords(RecordMetadata[] recordMetadatas) {
    return callOnRemoteMember(new FindMissedRecordsNodeCall(nodeAddress, recordMetadatas), false);
  }

  public NodeState state() {
    return callOnRemoteMember(new StateNodeCall(nodeAddress), false);
  }

  public ONodeAddress[] getSuccessors() {
    return callOnRemoteMember(new SuccessorsNodeCall(nodeAddress), false);
  }

  public void requestStabilization() {
    callOnRemoteMember(new RequestStabilizationNodeCall(nodeAddress), true);
  }

  @Override
  public Record createRecord(String data) {
    return callOnRemoteMember(new CreateNodeCall(nodeAddress, data), false);
  }

  @Override
  public Record createRecord(ORID id, String data) {
    return callOnRemoteMember(new CreateWithIdNodeCall(nodeAddress, id, data), false);
  }

  @Override
  public Record readRecord(ORID id) {
    return callOnRemoteMember(new GetNodeCall(nodeAddress, id), false);
  }

  @Override
  public void updateRecord(ORID id, Record record) {
    callOnRemoteMember(new UpdateNodeCall(nodeAddress, id, record), false);
  }

  @Override
  public void deleteRecord(ORID id, ODHTRecordVersion version) {
    callOnRemoteMember(new RemoveNodeCall(nodeAddress, id, version), false);
  }

  @Override
  public void updateReplica(Record replica, boolean async) {
    callOnRemoteMember(new UpdateReplicaNodeCall(nodeAddress, replica), async);
  }

  @Override
  public void updateReplicas(Record[] replicas, boolean async) {
    callOnRemoteMember(new UpdateReplicasNodeCall(nodeAddress, replicas), async);
  }

  @Override
  public Record getRecordFromNode(ORID id) {
    return callOnRemoteMember(new GetRecordFromNodeNodeCall(nodeAddress, id), false);
  }

  @Override
  public Record[] getRecordsFromNode(ORID[] ids) {
    return callOnRemoteMember(new GetRecordsFromNodeNodeCall(nodeAddress, ids), false);
  }

  @Override
  public RecordMetadata getRecordMetadataFromNode(ORID id) {
    return callOnRemoteMember(new GetRecordMetadataFromNodeNodeCall(nodeAddress, id), false);
  }

  @Override
  public RecordMetadata[] getRecordsForIntervalFromNode(ORID startId, ORID endId) {
    return callOnRemoteMember(new GetExistingRecordsForIntervalNodeCall(nodeAddress, startId, endId), false);
  }

  @Override
  public ODetachedMerkleTreeNode findMerkleTreeNode(ODetachedMerkleTreeNode node) {
    return callOnRemoteMember(new FindMerkleTreeNodeNodeCall(nodeAddress, node), false);
  }

  @Override
  public Record createRecordInNode(ORID id, String data) {
    return callOnRemoteMember(new CreateWithIdInNodeNodeCall(nodeAddress, id, data), false);
  }

  @Override
  public void updateRecordInNode(ORID id, Record record) {
    callOnRemoteMember(new UpdateRecordInNodeCall(nodeAddress, id, record), false);
  }

  @Override
  public void deleteRecordFromNode(ORID id, ODHTRecordVersion version) {
    callOnRemoteMember(new RemoveRecordFromNodeNodeCall(nodeAddress, id, version), false);
  }

	@Override
	public Record readRecordFromNode(ORID id) {
		return callOnRemoteMember(new ReadRecordFromNodeNodeCall(nodeAddress, id), false);
	}

	private <T> T callOnRemoteMember(final NodeCall<T> call, boolean async) {
    try {
      final Member member = findMember(nodeAddress);

      if (member == null)
        throw new ONodeOfflineException("Member " + nodeAddress + " was left.", null, nodeAddress.getNodeId());

      Future<T> future = (Future<T>) hazelcastInstance.getExecutorService().submit(new DistributedTask<T>(call, member));

      if (async)
        return null;

      return future.get();
    } catch (IllegalStateException ise) {
      if (!hazelcastInstance.getLifecycleService().isRunning())
        throw new ONodeOfflineException("Member " + nodeAddress + " was left.", ise, nodeAddress.getNodeId());
      throw ise;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new ORemoteNodeCallException("Error during remote call of node " + nodeAddress, e, nodeAddress.getNodeId());
    } catch (MemberLeftException mle) {
      throw new ONodeOfflineException("Member " + nodeAddress + " was left.", mle, nodeAddress.getNodeId());
    } catch (ExecutionException ee) {
      throw new ORemoteNodeCallException("Error during remote call of node " + nodeAddress, ee, nodeAddress.getNodeId());
    }
  }

  private Member findMember(OHazelcastNodeAddress address) {
    final Set<Member> members = hazelcastInstance.getCluster().getMembers();
    for (Member member : members)
      if (member.getUuid().equals(address.getMemberUUID()))
        return member;

    return null;
  }

  private static abstract class NodeCall<T> implements Callable<T>, Externalizable {
    protected OHazelcastNodeAddress nodeAddress;

    public NodeCall() {
    }

    public NodeCall(OHazelcastNodeAddress nodeAddress) {
      this.nodeAddress = nodeAddress;
    }

    public T call() throws Exception {
      ServerInstance serverInstance = ServerInstance.INSTANCES.get(nodeAddress.getMemberUUID());
      if (serverInstance == null)
        throw new ONodeOfflineException("Member " + nodeAddress + " was left.", null, nodeAddress.getNodeId());

      final ODHTNode node = serverInstance.findById(nodeAddress);
      if (node == null)
        throw new ONodeOfflineException("Member " + nodeAddress + " was left.", null, nodeAddress.getNodeId());

      return call(node);
    }

    protected abstract T call(ODHTNode node);

    public void writeExternal(ObjectOutput out) throws IOException {
      out.writeObject(nodeAddress);
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
      nodeAddress = (OHazelcastNodeAddress) in.readObject();
    }
  }

  private static final class FindMissedRecordsNodeCall extends NodeCall<ORID[]> {
    private RecordMetadata[] recordMetadatas;

    public FindMissedRecordsNodeCall() {
    }

    private FindMissedRecordsNodeCall(OHazelcastNodeAddress nodeAddress, RecordMetadata[] recordMetadatas) {
      super(nodeAddress);
      this.recordMetadatas = recordMetadatas;
    }

    @Override
    protected ORID[] call(ODHTNode node) {
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

    private UpdateReplicaNodeCall(OHazelcastNodeAddress nodeAddress, Record record) {
      super(nodeAddress);
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

    private UpdateReplicasNodeCall(OHazelcastNodeAddress nodeAddress, Record[] records) {
      super(nodeAddress);
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
    private ORID id;
    private Record    record;

    public UpdateNodeCall() {
    }

    private UpdateNodeCall(OHazelcastNodeAddress nodeAddress, ORID id, Record record) {
      super(nodeAddress);
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
      out.writeObject(id);
      out.writeObject(record);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
      super.readExternal(in);
      id = (ORID) in.readObject();
      record = (Record) in.readObject();
    }
  }

  private static final class UpdateRecordInNodeCall extends NodeCall<Void> {
    private ORID id;
    private Record    record;

    public UpdateRecordInNodeCall() {
    }

    private UpdateRecordInNodeCall(OHazelcastNodeAddress nodeAddress, ORID id, Record record) {
      super(nodeAddress);
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
      out.writeObject(id);
      out.writeObject(record);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
      super.readExternal(in);
      id = (ORID) in.readObject();
      record = (Record) in.readObject();
    }
  }

  private static final class CreateNodeCall extends NodeCall<Record> {
    private String data;

    public CreateNodeCall() {
    }

    private CreateNodeCall(OHazelcastNodeAddress nodeAddress, String data) {
      super(nodeAddress);
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
    private String    data;
    private ORID id;

    public CreateWithIdNodeCall() {
    }

    private CreateWithIdNodeCall(OHazelcastNodeAddress nodeAddress, ORID id, String data) {
      super(nodeAddress);
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
      out.writeObject(id);
      out.writeUTF(data);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
      super.readExternal(in);
      id = (ORID) in.readObject();
      data = in.readUTF();
    }
  }

  private static final class CreateWithIdInNodeNodeCall extends NodeCall<Record> {
    private String    data;
    private ORID id;

    public CreateWithIdInNodeNodeCall() {
    }

    private CreateWithIdInNodeNodeCall(OHazelcastNodeAddress nodeAddress, ORID id, String data) {
      super(nodeAddress);
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
      out.writeObject(id);
      out.writeUTF(data);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
      super.readExternal(in);
      id = (ORID) in.readObject();
      data = in.readUTF();
    }
  }

  private static final class GetSuccessorNodeCall extends NodeCall<ONodeAddress> {
    public GetSuccessorNodeCall() {
    }

    private GetSuccessorNodeCall(OHazelcastNodeAddress nodeAddress) {
      super(nodeAddress);
    }

    @Override
    protected ONodeAddress call(ODHTNode node) {
      return node.getSuccessor();
    }
  }

  private static final class SuccessorsNodeCall extends NodeCall<ONodeAddress[]> {
    public SuccessorsNodeCall() {
    }

    private SuccessorsNodeCall(OHazelcastNodeAddress nodeAddress) {
      super(nodeAddress);
    }

    @Override
    protected ONodeAddress[] call(ODHTNode node) {
      return node.getSuccessors();
    }
  }

  private static final class SizeNodeCall extends NodeCall<Integer> {
    public SizeNodeCall() {
    }

    private SizeNodeCall(OHazelcastNodeAddress nodeAddress) {
      super(nodeAddress);
    }

    @Override
    protected Integer call(ODHTNode node) {
      return node.size();
    }
  }

  private static final class StateNodeCall extends NodeCall<NodeState> {
    public StateNodeCall() {
    }

    private StateNodeCall(OHazelcastNodeAddress nodeAddress) {
      super(nodeAddress);
    }

    @Override
    protected NodeState call(ODHTNode node) {
      return node.state();
    }
  }

  private static final class GetPredecessorNodeCall extends NodeCall<ONodeAddress> {
    public GetPredecessorNodeCall() {
    }

    private GetPredecessorNodeCall(OHazelcastNodeAddress nodeAddress) {
      super(nodeAddress);
    }

    @Override
    protected ONodeAddress call(ODHTNode node) {
      return node.getPredecessor();
    }
  }

  private static final class JoinNodeCall extends NodeCall<Boolean> {
    private ONodeAddress joinNodeAddress;

    public JoinNodeCall() {
    }

    private JoinNodeCall(OHazelcastNodeAddress nodeAddress, ONodeAddress joinNodeAddress) {
      super(nodeAddress);
      this.joinNodeAddress = joinNodeAddress;
    }

    @Override
    protected Boolean call(ODHTNode node) {
      return node.joinDHT(joinNodeAddress);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
      super.writeExternal(out);
      out.writeObject(joinNodeAddress);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
      super.readExternal(in);
      joinNodeAddress = (ONodeAddress) in.readObject();
    }
  }

  private static final class NotifyNodeCall extends NodeCall<ONodeAddress> {
    private ONodeAddress notifyNodeAddress;

    public NotifyNodeCall() {
    }

    private NotifyNodeCall(OHazelcastNodeAddress nodeAddress, ONodeAddress notifyNodeAddress) {
      super(nodeAddress);
      this.notifyNodeAddress = notifyNodeAddress;
    }

    @Override
    protected ONodeAddress call(ODHTNode node) {
      return node.notifyParent(notifyNodeAddress);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
      super.writeExternal(out);
      out.writeObject(notifyNodeAddress);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
      super.readExternal(in);
      notifyNodeAddress = (ONodeAddress) in.readObject();
    }
  }

  private static final class FindMerkleTreeNodeNodeCall extends NodeCall<ODetachedMerkleTreeNode> {
    private ODetachedMerkleTreeNode merkleTreeNode;

    public FindMerkleTreeNodeNodeCall() {
    }

    private FindMerkleTreeNodeNodeCall(OHazelcastNodeAddress nodeAddress, ODetachedMerkleTreeNode merkleTreeNode) {
      super(nodeAddress);
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

  private static final class FindSuccessorNodeCall extends NodeCall<ONodeAddress> {
    private ONodeId keyId;

    public FindSuccessorNodeCall() {
    }

    private FindSuccessorNodeCall(OHazelcastNodeAddress nodeAddress, ONodeId keyId) {
      super(nodeAddress);
      this.keyId = keyId;
    }

    @Override
    protected ONodeAddress call(ODHTNode node) {
      return node.findSuccessor(keyId);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
      super.writeExternal(out);
      out.writeObject(keyId);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
      super.readExternal(in);
      keyId = (ONodeId) in.readObject();
    }
  }

  private static final class GetNodeCall extends NodeCall<Record> {
    private ORID id;

    public GetNodeCall() {
    }

    private GetNodeCall(OHazelcastNodeAddress nodeAddress, ORID id) {
      super(nodeAddress);
      this.id = id;
    }

    @Override
    protected Record call(ODHTNode node) {
      return node.readRecord(id);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
      super.writeExternal(out);
      out.writeObject(id);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
      super.readExternal(in);
      id = (ORID) in.readObject();
    }
  }

  private static final class GetExistingRecordsForIntervalNodeCall extends NodeCall<RecordMetadata[]> {
    private ORID startId;
    private ORID endId;

    public GetExistingRecordsForIntervalNodeCall() {
    }

    private GetExistingRecordsForIntervalNodeCall(OHazelcastNodeAddress nodeAddress, ORID startId, ORID endId) {
      super(nodeAddress);

      this.startId = startId;
      this.endId = endId;
    }

    @Override
    protected RecordMetadata[] call(ODHTNode node) {
      return node.getRecordsForIntervalFromNode(startId, endId);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
      super.writeExternal(out);

      out.writeObject(startId);
      out.writeObject(endId);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
      super.readExternal(in);

      startId = (ORID) in.readObject();
      endId = (ORID) in.readObject();
    }
  }

  private static final class GetRecordFromNodeNodeCall extends NodeCall<Record> {
    private ORID id;

    public GetRecordFromNodeNodeCall() {
    }

    private GetRecordFromNodeNodeCall(OHazelcastNodeAddress nodeAddress, ORID id) {
      super(nodeAddress);
      this.id = id;
    }

    @Override
    protected Record call(ODHTNode node) {
      return node.getRecordFromNode(id);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
      super.writeExternal(out);
      out.writeObject(id);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
      super.readExternal(in);
      id = (ORID) in.readObject();
    }
  }

	private static final class ReadRecordFromNodeNodeCall extends NodeCall<Record> {
		private ORID id;

		public ReadRecordFromNodeNodeCall() {
		}

		private ReadRecordFromNodeNodeCall(OHazelcastNodeAddress nodeAddress, ORID id) {
			super(nodeAddress);
			this.id = id;
		}

		@Override
		protected Record call(ODHTNode node) {
			return node.readRecordFromNode(id);
		}

		@Override
		public void writeExternal(ObjectOutput out) throws IOException {
			super.writeExternal(out);
			out.writeObject(id);
		}

		@Override
		public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
			super.readExternal(in);
			id = (ORID) in.readObject();
		}
	}


	private static final class GetRecordsFromNodeNodeCall extends NodeCall<Record[]> {
    private ORID[] ids;

    public GetRecordsFromNodeNodeCall() {
    }

    private GetRecordsFromNodeNodeCall(OHazelcastNodeAddress nodeAddress, ORID[] ids) {
      super(nodeAddress);
      this.ids = ids;
    }

    @Override
    protected Record[] call(ODHTNode node) {
      return node.getRecordsFromNode(ids);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
      super.writeExternal(out);

      out.writeInt(ids.length);
      for (ORID id : ids)
        out.writeObject(id);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
      super.readExternal(in);
      int len = in.readInt();
      ids = new ORID[len];

      for (int i = 0; i < len; i++)
        ids[i] = (ORID) in.readObject();
    }
  }

  private static final class GetRecordMetadataFromNodeNodeCall extends NodeCall<RecordMetadata> {
    private ORID id;

    public GetRecordMetadataFromNodeNodeCall() {
    }

    private GetRecordMetadataFromNodeNodeCall(OHazelcastNodeAddress nodeAddress, ORID id) {
      super(nodeAddress);
      this.id = id;
    }

    @Override
    protected RecordMetadata call(ODHTNode node) {
      return node.getRecordMetadataFromNode(id);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
      super.writeExternal(out);
      out.writeObject(id);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
      super.readExternal(in);
      id = (ORID) in.readObject();
    }
  }

  private static final class RemoveNodeCall extends NodeCall<Void> {
    private ORID         id;
    private ODHTRecordVersion version;

    public RemoveNodeCall() {
    }

    private RemoveNodeCall(OHazelcastNodeAddress nodeAddress, ORID id, ODHTRecordVersion version) {
      super(nodeAddress);
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
      out.writeObject(id);
      out.writeObject(version);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
      super.readExternal(in);
      id = (ORID) in.readObject();
      version = (ODHTRecordVersion) in.readObject();
    }
  }

  private static final class RemoveRecordFromNodeNodeCall extends NodeCall<Void> {
    private ORID         id;
    private ODHTRecordVersion version;

    public RemoveRecordFromNodeNodeCall() {
    }

    private RemoveRecordFromNodeNodeCall(OHazelcastNodeAddress nodeAddress, ORID id, ODHTRecordVersion version) {
      super(nodeAddress);
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
      out.writeObject(id);
      out.writeObject(version);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
      super.readExternal(in);
      id = (ORID) in.readObject();
      version = (ODHTRecordVersion) in.readObject();
    }
  }

  private static final class RequestStabilizationNodeCall extends NodeCall<Void> {
    public RequestStabilizationNodeCall() {
    }

    private RequestStabilizationNodeCall(OHazelcastNodeAddress nodeAddress) {
      super(nodeAddress);
    }

    @Override
    protected Void call(ODHTNode node) {
      node.requestStabilization();
      return null;
    }
  }
}
