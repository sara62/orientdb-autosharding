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
import com.orientechnologies.orient.server.distributed.*;

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

  public long getSuccessor() {
    return callOnRemoteMember(new GetSuccessorNodeCall(nodeId, member.getUuid()), false);
  }

  public Long getPredecessor() {
    return callOnRemoteMember(new GetPredecessorNodeCall(nodeId, member.getUuid()), false);
  }

  public long notifyParent(long nodeId) {
    return callOnRemoteMember(new NotifyNodeCall(nodeId, member.getUuid(), nodeId), false);
  }

  public boolean join(long nodeId) {
    return callOnRemoteMember(new JoinNodeCall(nodeId, member.getUuid(), nodeId), false);
  }

  public long findSuccessor(long id) {
    return callOnRemoteMember(new FindSuccessorNodeCall(nodeId, member.getUuid(), id), false);
  }

  public int size() {
    return callOnRemoteMember(new SizeNodeCall(nodeId, member.getUuid()), false);
  }

  public long[] findMissedRecords(long[] ids, ODHTRecordVersion[] versions) {
    return callOnRemoteMember(new FindMissedRecordsNodeCall(nodeId, member.getUuid(), ids, versions), false);
  }

  public NodeState state() {
    return callOnRemoteMember(new StateNodeCall(nodeId, member.getUuid()), false);
  }

  public long[] getSuccessors(int depth) {
    return callOnRemoteMember(new SuccessorsNodeCall(nodeId, member.getUuid(), depth), false);
  }

  public void requestStabilization() {
    callOnRemoteMember(new RequestStabilizationNodeCall(nodeId, member.getUuid()), true);
  }

  @Override
  public Record create(String data) {
    return callOnRemoteMember(new CreateNodeCall(nodeId, member.getUuid(), data), false);
  }

  @Override
  public Record create(long id, String data) {
    return callOnRemoteMember(new CreateWithIdNodeCall(nodeId, member.getUuid(), id, data), false);
  }

  @Override
  public Record get(long id) {
    return callOnRemoteMember(new GetNodeCall(nodeId, member.getUuid(), id), false);
  }

  @Override
  public void update(long id, Record record) {
    callOnRemoteMember(new UpdateNodeCall(nodeId, member.getUuid(), id, record), false);
  }

  @Override
  public void remove(long id, int version) {
    callOnRemoteMember(new RemoveNodeCall(nodeId, member.getUuid(), id, version), false);
  }

	@Override
	public void updateReplica(Record replica) {
		callOnRemoteMember(new UpdateReplicaNodeCall(nodeId, member.getUuid(), replica), false);
	}

	@Override
  public Record getRecordFromNode(long id) {
    return callOnRemoteMember(new GetRecordFromNodeNodeCall(nodeId, member.getUuid(), id), true);
  }

  private <T> T callOnRemoteMember(final NodeCall<T> call, boolean async) {
    try {
      Future<T> future = (Future<T>) hazelcastInstance.getExecutorService().submit(new DistributedTask<T>(call, member));

      if (async)
        return null;

      return future.get();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } catch (MemberLeftException mle) {
      throw new ONodeOfflineException("Member with id " + nodeId + " was left.", mle, nodeId);
    } catch (ExecutionException ee) {
      if (ee.getCause() instanceof ODHTKeyOwnerIsAbsentException)
        throw new ODHTKeyOwnerIsAbsentException(ee.getCause(), ((ODHTKeyOwnerIsAbsentException) ee.getCause()).getKey());

      throw new ORemoteNodeCallException("Error during remote call of node " + nodeId, ee, nodeId);
    }

    return null;
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
      final ODHTNode node = ServerInstance.INSTANCES.get(memberUUID).findById(nodeId);
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
		private long[] ids;
		private ODHTRecordVersion[] versions;

		public FindMissedRecordsNodeCall() {
		}

		private FindMissedRecordsNodeCall(long nodeId, String memberUUID, long[] ids, ODHTRecordVersion[] versions) {
			super(nodeId, memberUUID);
			this.ids = ids;
			this.versions = versions;
		}

		@Override
		protected long[] call(ODHTNode node) {
			return node.findMissedRecords(ids, versions);
		}

		@Override
		public void writeExternal(ObjectOutput out) throws IOException {
			super.writeExternal(out);

			out.writeInt(ids.length);

			for (long id : ids)
				out.writeLong(id);

			for (ODHTRecordVersion version : versions)
				out.writeObject(version);
  	}

		@Override
		public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
			super.readExternal(in);

			final int dataLength = in.readInt();
			versions = new ODHTRecordVersion[dataLength];
			ids = new long[dataLength];

			for (int i = 0; i < dataLength; i++)
				ids[i] = in.readLong();

			for (int i = 0; i < dataLength; i++)
				versions[i] = (ODHTRecordVersion)in.readObject();
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
			node.updateReplica(record);

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
      node.update(id, record);
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
      return node.create(data);
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
      return node.create(id, data);
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
    private int depth;

    public SuccessorsNodeCall() {
    }

    private SuccessorsNodeCall(long nodeId, String memberUUID, int depth) {
      super(nodeId, memberUUID);
      this.depth = depth;
    }

    @Override
    protected long[] call(ODHTNode node) {
      return node.getSuccessors(depth);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
      super.writeExternal(out);
      out.writeInt(depth);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
      super.readExternal(in);
      depth = in.readInt();
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
      return node.join(joinNodeId);
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
      return node.get(id);
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

  private static final class GetRecordFromNodeNodeCall extends NodeCall<Record> {
    private long id;

    public GetRecordFromNodeNodeCall() {
    }

    private GetRecordFromNodeNodeCall(long nodeId, String memberUUID, long id) {
      super(nodeId, memberUUID);
      this.id = id;
    }

    @Override
    protected Record call(ODHTNode node) {
      return node.getRecordFromNode(id);
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
    private long id;
    private int  version;

    public RemoveNodeCall() {
    }

    private RemoveNodeCall(long nodeId, String memberUUID, long id, int version) {
      super(nodeId, memberUUID);
      this.id = id;
      this.version = version;
    }

    @Override
    protected Void call(ODHTNode node) {
      node.remove(id, version);
      return null;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
      super.writeExternal(out);
      out.writeLong(id);
      out.writeInt(version);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
      super.readExternal(in);
      id = in.readLong();
      version = in.readInt();
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
