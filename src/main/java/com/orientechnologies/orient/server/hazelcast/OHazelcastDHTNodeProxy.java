package com.orientechnologies.orient.server.hazelcast;

import com.hazelcast.core.DistributedTask;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;
import com.orientechnologies.orient.server.distributed.ODHTNode;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * @author Andrey Lomakin
 * @since 17.08.12
 */
public class OHazelcastDHTNodeProxy implements ODHTNode {
	private final long nodeId;
	private final Member member;
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

	public void notify(long node) {
		callOnRemoteMember(new NotifyNodeCall(nodeId, member.getUuid(), node), true);
	}

	public boolean join(long node) {
		return callOnRemoteMember(new JoinNodeCall(nodeId, member.getUuid(), node), false);
	}

	public long findSuccessor(long id) {
		return callOnRemoteMember(new FindSuccessorNodeCall(nodeId, member.getUuid(), id), false);
	}

	public void put(Long id, String data) {
		callOnRemoteMember(new PutNodeCall(nodeId,member.getUuid(), data, id), false);
	}

	public String get(Long id) {
		return callOnRemoteMember(new GetNodeCall(nodeId, member.getUuid(), id), false);
	}

	public int size() {
		return callOnRemoteMember(new SizeNodeCall(nodeId, member.getUuid()), false);
	}

	public void notifyMigrationEnd(long notifierId) {
		callOnRemoteMember(new NotifyMigrationEndNodeCall(this.nodeId, member.getUuid(), notifierId), true);
	}

	public boolean remove(Long id) {
		return callOnRemoteMember(new RemoveNodeCall(nodeId, member.getUuid(), id), false);
	}

	public void requestMigration(long requesterId) {
		callOnRemoteMember(new RequestMigrationNodeCall(nodeId, member.getUuid(), requesterId), true);
	}

	private <T> T callOnRemoteMember(final NodeCall<T> call, boolean async) {
		try {
			Future<T> future =
							(Future<T>)hazelcastInstance.getExecutorService().submit(new DistributedTask<T>(call, member));

			if (async)
				return null;

			return future.get();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ExecutionException e) {
			e.printStackTrace();
		}

		return null;
	}

	private static abstract class NodeCall<T> implements Callable<T>, Externalizable {
		protected long nodeId;
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

	private static final class NotifyNodeCall extends NodeCall<Void> {
		private long notifyNodeId;

		public NotifyNodeCall() {
		}

		private NotifyNodeCall(long nodeId, String memberUUID, long notifyNodeId) {
			super(nodeId, memberUUID);
			this.notifyNodeId = notifyNodeId;
		}

		@Override
		protected Void call(ODHTNode node) {
			node.notify(notifyNodeId);

			return null;
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

	private static final class PutNodeCall extends NodeCall<Void> {
		private String data;
		private Long keyId;

		public PutNodeCall() {
		}

		private PutNodeCall(long nodeId, String memberUUID, String data, Long keyId) {
			super(nodeId, memberUUID);
			this.data = data;
			this.keyId = keyId;
		}

		@Override
		protected Void call(ODHTNode node) {
			node.put(keyId, data);

			return null;
		}

		@Override
		public void writeExternal(ObjectOutput out) throws IOException {
			super.writeExternal(out);
			out.writeLong(keyId);
			out.writeUTF(data);
		}

		@Override
		public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
			super.readExternal(in);
			keyId = in.readLong();
			data = in.readUTF();
		}
	}

	private static final class GetNodeCall extends NodeCall<String> {
		private Long keyId;

		public GetNodeCall() {
		}

		private GetNodeCall(long nodeId, String memberUUID, Long keyId) {
			super(nodeId, memberUUID);
			this.keyId = keyId;
		}

		@Override
		protected String call(ODHTNode node) {
			return node.get(keyId);
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

	private static final class RemoveNodeCall extends NodeCall<Boolean> {
		private long keyId;

		public RemoveNodeCall() {
		}

		private RemoveNodeCall(long nodeId, String memberUUID, long keyId) {
			super(nodeId, memberUUID);
			this.keyId = keyId;
		}

		@Override
		protected Boolean call(ODHTNode node) {
			return node.remove(keyId);
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

	private static final class NotifyMigrationEndNodeCall extends NodeCall<Void> {
		private long notifierId;

		public NotifyMigrationEndNodeCall() {
		}

		private NotifyMigrationEndNodeCall(long nodeId, String memberUUID, long notifierId) {
			super(nodeId, memberUUID);
			this.notifierId = notifierId;
		}

		@Override
		protected Void call(ODHTNode node) {
			node.notifyMigrationEnd(notifierId);
			return null;
		}

		@Override
		public void writeExternal(ObjectOutput out) throws IOException {
			super.writeExternal(out);
			out.writeLong(notifierId);
		}

		@Override
		public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
			super.readExternal(in);
			notifierId = in.readLong();
		}
	}

	private static final class RequestMigrationNodeCall extends NodeCall<Void> {
		private long requesterId;

		public RequestMigrationNodeCall() {
		}

		private RequestMigrationNodeCall(long nodeId, String memberUUID, long requesterId) {
			super(nodeId, memberUUID);
			this.requesterId = requesterId;
		}

		@Override
		protected Void call(ODHTNode node) {
			node.requestMigration(requesterId);
			return null;
		}

		@Override
		public void writeExternal(ObjectOutput out) throws IOException {
			super.writeExternal(out);
			out.writeLong(requesterId);
		}

		@Override
		public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
			super.readExternal(in);
			requesterId = in.readLong();
		}
	}

}