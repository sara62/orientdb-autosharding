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
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.version.ORecordVersion;
import com.orientechnologies.orient.server.distributed.ODHTNode;
import com.orientechnologies.orient.server.distributed.ONodeAddress;
import com.orientechnologies.orient.server.distributed.ONodeOfflineException;
import com.orientechnologies.orient.server.distributed.ORecordMetadata;
import com.orientechnologies.orient.server.distributed.Record;
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

  public int size(String storageName, int clusterId) {
    return callOnRemoteMember(new SizeNodeCall(nodeAddress, storageName), false);
  }

  public ORID[] findMissedRecords(String storageName, ORecordMetadata[] recordMetadatas) {
    return callOnRemoteMember(new FindMissedRecordsNodeCall(nodeAddress, recordMetadatas, storageName), false);
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
  public ORecordInternal<?> createRecord(String storageName, ORecordInternal<?> record) {
    return callOnRemoteMember(new CreateNodeCall(nodeAddress, storageName, record), false);
  }

  @Override
  public ORecordInternal<?> readRecord(String storageName, ORID id) {
    return callOnRemoteMember(new GetNodeCall(nodeAddress, storageName, id), false);
  }

  @Override
  public ORecordInternal<?> updateRecord(String storageName, ORecordInternal<?> record) {
    return callOnRemoteMember(new UpdateNodeCall(nodeAddress, storageName, record), false);
  }

  @Override
  public void deleteRecord(String storageName, ORID id, ORecordVersion version) {
    callOnRemoteMember(new RemoveNodeCall(nodeAddress, storageName, id, version), false);
  }

  @Override
  public void updateReplica(String storageName, ORecordInternal<?> replica, boolean async) {
    callOnRemoteMember(new UpdateReplicaNodeCall(nodeAddress, storageName, replica), async);
  }

  @Override
  public void updateReplicas(String storageName, ORecordInternal<?>[] replicas, boolean async) {
    callOnRemoteMember(new UpdateReplicasNodeCall(nodeAddress, storageName, replicas), async);
  }

  @Override
  public ORecordInternal<?> getRecordFromNode(String storageName, ORID id) {
    return callOnRemoteMember(new GetRecordFromNodeNodeCall(nodeAddress, id, storageName), false);
  }

  @Override
  public ORecordInternal<?>[] getRecordsFromNode(String storageName, ORID[] ids) {
    return callOnRemoteMember(new GetRecordsFromNodeNodeCall(nodeAddress, ids), false);
  }

  @Override
  public ORecordMetadata getRecordMetadataFromNode(String storageName, ORID id) {
    return callOnRemoteMember(new GetRecordMetadataFromNodeNodeCall(nodeAddress, id), false);
  }

  @Override
  public ORecordMetadata[] getRecordsForIntervalFromNode(String storageName, ORID startId, ORID endId) {
    return callOnRemoteMember(new GetExistingRecordsForIntervalNodeCall(nodeAddress, startId, endId), false);
  }

  @Override
  public ODetachedMerkleTreeNode findMerkleTreeNode(String storageName, ODetachedMerkleTreeNode node) {
    return callOnRemoteMember(new FindMerkleTreeNodeNodeCall(nodeAddress, node), false);
  }

  @Override
  public ORecordInternal<?> createRecordInNode(String storageName, ORecordInternal<?> record) {
    return callOnRemoteMember(new CreateWithIdInNodeNodeCall(nodeAddress, storageName, record), false);
  }

  @Override
  public ORecordInternal<?> updateRecordInNode(String storageName, ORecordInternal<?> record) {
    return callOnRemoteMember(new UpdateRecordInNodeCall(nodeAddress, storageName, record), false);
  }

  @Override
  public void deleteRecordFromNode(String storageName, ORID id, ORecordVersion version) {
    callOnRemoteMember(new RemoveRecordFromNodeNodeCall(nodeAddress, id, version), false);
  }

	@Override
	public ORecordInternal<?> readRecordFromNode(String storageName, ORID id) {
		return callOnRemoteMember(new ReadRecordFromNodeNodeCall(nodeAddress, id), false);
	}

	private <T> T callOnRemoteMember(final NodeCall<T> call, boolean async) {
    try {
      final Member member = findMember(nodeAddress);

      if (member == null)
        throw new ONodeOfflineException("Member " + nodeAddress + " was left.", null, nodeAddress.getNodeId());

      @SuppressWarnings("unchecked")
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
    private ORecordMetadata[] recordMetadatas;
    private String storageName;

    public FindMissedRecordsNodeCall() {
    }

    private FindMissedRecordsNodeCall(OHazelcastNodeAddress nodeAddress, ORecordMetadata[] recordMetadatas, String storageName) {
      super(nodeAddress);
      this.recordMetadatas = recordMetadatas;
      this.storageName = storageName;
    }

    @Override
    protected ORID[] call(ODHTNode node) {
      return node.findMissedRecords(storageName, recordMetadatas);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
      super.writeExternal(out);
      out.writeUTF(storageName);

      out.writeInt(recordMetadatas.length);

      for (ORecordMetadata recordMetadata : recordMetadatas)
        out.writeObject(recordMetadata);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
      super.readExternal(in);
      storageName = in.readUTF();

      final int dataLength = in.readInt();
      recordMetadatas = new ORecordMetadata[dataLength];

      for (int i = 0; i < dataLength; i++)
        recordMetadatas[i] = (ORecordMetadata) in.readObject();
    }
  }

  private static final class UpdateReplicaNodeCall extends NodeCall<Void> {
    private ORecordInternal<?> record;
    private String storageName;

    public UpdateReplicaNodeCall() {
    }

    private UpdateReplicaNodeCall(OHazelcastNodeAddress nodeAddress, String storageName, ORecordInternal<?> record) {
      super(nodeAddress);
      this.storageName = storageName;
      this.record = record;
    }

    @Override
    protected Void call(ODHTNode node) {
      node.updateReplica(storageName, record, false);
      return null;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
      super.writeExternal(out);
      out.writeUTF(storageName);
      out.writeObject(record);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
      super.readExternal(in);
      storageName = in.readUTF();
      record = (ORecordInternal) in.readObject();
    }
  }

  private static final class UpdateReplicasNodeCall extends NodeCall<Void> {
    private ORecordInternal<?>[] records;
    private String storageName;

    public UpdateReplicasNodeCall() {
    }

    private UpdateReplicasNodeCall(OHazelcastNodeAddress nodeAddress, String storageName, ORecordInternal<?>[] records) {
      super(nodeAddress);
      this.storageName = storageName;
      this.records = records;
    }

    @Override
    protected Void call(ODHTNode node) {
      node.updateReplicas(null, records, false);

      return null;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
      super.writeExternal(out);
      out.writeUTF(storageName);
      out.writeInt(records.length);
      for (ORecordInternal<?> record : records)
        out.writeObject(record);

    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
      super.readExternal(in);
      storageName = in.readUTF();
      int len = in.readInt();
      records = new ORecordInternal<?>[len];
      for (int i = 0; i < len; i++)
        records[i] = (ORecordInternal<?>) in.readObject();
    }
  }

  private static final class UpdateNodeCall extends NodeCall<ORecordInternal<?>> {
    private ORecordInternal<?> record;
    private String storageName;

    public UpdateNodeCall() {
    }

    private UpdateNodeCall(OHazelcastNodeAddress nodeAddress, String storageName, ORecordInternal<?> record) {
      super(nodeAddress);
      this.storageName = storageName;
      this.record = record;
    }

    @Override
    protected ORecordInternal<?> call(ODHTNode node) {
      return node.updateRecord(storageName, record);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
      super.writeExternal(out);

      out.writeObject(record);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
      super.readExternal(in);
      record = (ORecordInternal<?>) in.readObject();
    }
  }

  private static final class UpdateRecordInNodeCall extends NodeCall<ORecordInternal> {
    private ORecordInternal<?>    record;

    public UpdateRecordInNodeCall() {
    }

    private UpdateRecordInNodeCall(OHazelcastNodeAddress nodeAddress, String storageName, ORecordInternal<?> record) {
      super(nodeAddress);
      this.record = record;
    }

    @Override
    protected ORecordInternal<?> call(ODHTNode node) {
      return node.updateRecordInNode(null, record);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
      super.writeExternal(out);
      out.writeObject(record);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
      super.readExternal(in);
      record = (ORecordInternal<?>) in.readObject();
    }
  }

  private static final class CreateNodeCall extends NodeCall<ORecordInternal<?>> {
    private ORecordInternal<?>    data;

    public CreateNodeCall() {
    }

    private CreateNodeCall(OHazelcastNodeAddress nodeAddress, String storageName, ORecordInternal<?> data) {
      super(nodeAddress);
      this.data = data;
    }

    @Override
    protected ORecordInternal<?> call(ODHTNode node) {
      return node.createRecord(null, data);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
      super.writeExternal(out);
      out.writeObject(data);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
      super.readExternal(in);
      data = (ORecordInternal<?>) in.readObject();
    }
  }

  private static final class CreateWithIdInNodeNodeCall extends NodeCall<ORecordInternal<?>> {
    private String storageName;
    private ORecordInternal<?> record;

    public CreateWithIdInNodeNodeCall() {
    }

    private CreateWithIdInNodeNodeCall(OHazelcastNodeAddress nodeAddress, String storageName, ORecordInternal<?> record) {
      super(nodeAddress);
      this.storageName = storageName;
      this.record = record;
    }

    @Override
    protected ORecordInternal<?> call(ODHTNode node) {
      return node.createRecordInNode(null, null);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
      super.writeExternal(out);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
      super.readExternal(in);
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

    private SizeNodeCall(OHazelcastNodeAddress nodeAddress, String storageName) {
      super(nodeAddress);
    }

    @Override
    protected Integer call(ODHTNode node) {
      return node.size(null, -1);
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
      return node.findMerkleTreeNode(null, merkleTreeNode);
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

  private static final class GetNodeCall extends NodeCall<ORecordInternal<?>> {
    private ORID id;

    public GetNodeCall() {
    }

    private GetNodeCall(OHazelcastNodeAddress nodeAddress, String storageName, ORID id) {
      super(nodeAddress);
      this.id = id;
    }

    @Override
    protected ORecordInternal<?> call(ODHTNode node) {
      return node.readRecord(null, id);
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

  private static final class GetExistingRecordsForIntervalNodeCall extends NodeCall<ORecordMetadata[]> {
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
    protected ORecordMetadata[] call(ODHTNode node) {
      return node.getRecordsForIntervalFromNode(null, startId, endId);
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

  private static final class GetRecordFromNodeNodeCall extends NodeCall<ORecordInternal<?>> {
    private ORID id;

    public GetRecordFromNodeNodeCall() {
    }

    private GetRecordFromNodeNodeCall(OHazelcastNodeAddress nodeAddress, ORID id, String storageName) {
      super(nodeAddress);
      this.id = id;
    }

    @Override
    protected ORecordInternal<?> call(ODHTNode node) {
      return node.getRecordFromNode(null, id);
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

	private static final class ReadRecordFromNodeNodeCall extends NodeCall<ORecordInternal<?>> {
		private ORID id;

		public ReadRecordFromNodeNodeCall() {
		}

		private ReadRecordFromNodeNodeCall(OHazelcastNodeAddress nodeAddress, ORID id) {
			super(nodeAddress);
			this.id = id;
		}

		@Override
		protected ORecordInternal<?> call(ODHTNode node) {
			return node.readRecordFromNode(null, id);
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


	private static final class GetRecordsFromNodeNodeCall extends NodeCall<ORecordInternal<?>[]> {
    private ORID[] ids;

    public GetRecordsFromNodeNodeCall() {
    }

    private GetRecordsFromNodeNodeCall(OHazelcastNodeAddress nodeAddress, ORID[] ids) {
      super(nodeAddress);
      this.ids = ids;
    }

    @Override
    protected ORecordInternal<?>[] call(ODHTNode node) {
      return node.getRecordsFromNode(null, ids);
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

  private static final class GetRecordMetadataFromNodeNodeCall extends NodeCall<ORecordMetadata> {
    private ORID id;

    public GetRecordMetadataFromNodeNodeCall() {
    }

    private GetRecordMetadataFromNodeNodeCall(OHazelcastNodeAddress nodeAddress, ORID id) {
      super(nodeAddress);
      this.id = id;
    }

    @Override
    protected ORecordMetadata call(ODHTNode node) {
      return node.getRecordMetadataFromNode(null, id);
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
    private ORecordVersion version;

    public RemoveNodeCall() {
    }

    private RemoveNodeCall(OHazelcastNodeAddress nodeAddress, String storageName, ORID id, ORecordVersion version) {
      super(nodeAddress);
      this.id = id;
      this.version = version;
    }

    @Override
    protected Void call(ODHTNode node) {
      node.deleteRecord(null, id, version);
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
      version = (ORecordVersion) in.readObject();
    }
  }

  private static final class RemoveRecordFromNodeNodeCall extends NodeCall<Void> {
    private ORID         id;
    private ORecordVersion version;

    public RemoveRecordFromNodeNodeCall() {
    }

    private RemoveRecordFromNodeNodeCall(OHazelcastNodeAddress nodeAddress, ORID id, ORecordVersion version) {
      super(nodeAddress);
      this.id = id;
      this.version = version;
    }

    @Override
    protected Void call(ODHTNode node) {
      node.deleteRecordFromNode(null, id, version);
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
      version = (ORecordVersion) in.readObject();
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
