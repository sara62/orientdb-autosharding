package com.orientechnologies.orient.server.hazelcast;

import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.core.MembershipListener;
import com.orientechnologies.common.hash.OMurmurHash3;
import com.orientechnologies.orient.server.distributed.ODHTNode;
import com.orientechnologies.orient.server.distributed.ODHTNodeLookup;
import com.orientechnologies.orient.server.distributed.OLocalDHTNode;

import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Andrey Lomakin
 * @since 15.08.12
 */
public class ServerInstance implements MembershipListener, ODHTNodeLookup {
	public static final Map<String, ServerInstance> INSTANCES = new ConcurrentHashMap<String, ServerInstance>();

	private final ConcurrentHashMap<Long, Member> idMemberMap = new ConcurrentHashMap<Long, Member>();
	private volatile OLocalDHTNode localNode;
	private volatile HazelcastInstance hazelcastInstance;
	private final Timer timer = new Timer("DHT timer", true);

	public ServerInstance() {
	}

	public void init() {
		XmlConfigBuilder xmlConfigBuilder = new XmlConfigBuilder(ServerInstance.class.getResourceAsStream("/hazelcast.xml"));

		hazelcastInstance = Hazelcast.newHazelcastInstance(xmlConfigBuilder.build());
		localNode = new OLocalDHTNode(getNodeId(hazelcastInstance.getCluster().getLocalMember()));
		localNode.setNodeLookup(this);
		INSTANCES.put(hazelcastInstance.getCluster().getLocalMember().getUuid(), this);

		hazelcastInstance.getCluster().addMembershipListener(this);

		for (final Member member : hazelcastInstance.getCluster().getMembers()) {
			final long nodeId = getNodeId(member);
			if (nodeId != localNode.getNodeId())
				idMemberMap.put(nodeId, member);
		}

		if (idMemberMap.isEmpty())
			localNode.create();
		else {
			long oldestNodeId = getNodeId(hazelcastInstance.getCluster().getMembers().iterator().next());

			while (!localNode.join(oldestNodeId))
				oldestNodeId = getNodeId(hazelcastInstance.getCluster().getMembers().iterator().next());
		}

		timer.schedule(new TimerTask() {
			@Override
			public void run() {
				localNode.stabilize();
				localNode.fixFingers();
			}
		}, 10000, 10000);
	}

	public void put(long key, String value) {
		localNode.put(key, value);
	}

	public String get(long key) {
		return localNode.get(key);
	}

	public boolean remove(long key) {
		return localNode.remove(key);
	}

	public void memberAdded(MembershipEvent membershipEvent) {
		final Member member = membershipEvent.getMember();
		final long nodeId = getNodeId(member);

		idMemberMap.put(nodeId, member);
		localNode.stabilize();
		localNode.fixFingers();
	}

	public void memberRemoved(MembershipEvent membershipEvent) {
		final Member member = membershipEvent.getMember();
		final long nodeId = getNodeId(member);

		idMemberMap.remove(nodeId);
		localNode.fixPredecessor();
		localNode.stabilize();
	}

	public ODHTNode findById(long id) {
		if (localNode.getNodeId() == id)
			return localNode;

		final Member member = idMemberMap.get(id);
		if (member == null)
			return null;

		return new OHazelcastDHTNodeProxy(id, member, hazelcastInstance);
	}

	protected long getNodeId(final Member iMember) {
		final String address = iMember.getInetSocketAddress().toString();
		final long nodeId = OMurmurHash3.murmurHash3_x64_64(address.getBytes(), 0);
		if (nodeId < 0)
			return -nodeId;

		return nodeId;
	}

	public ODHTNode findSuccessor(int id) {
		return findById(localNode.findSuccessor(id));
	}
}