package com.orientechnologies.orient.server.hazelcast;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.DistributedTask;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleListener;
import com.hazelcast.core.Member;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.core.MembershipListener;
import com.orientechnologies.orient.core.id.ONodeId;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.server.distributed.ODHTNode;
import com.orientechnologies.orient.server.distributed.ODHTNodeLookup;
import com.orientechnologies.orient.server.distributed.ODHTRecordVersion;
import com.orientechnologies.orient.server.distributed.OLocalDHTNode;
import com.orientechnologies.orient.server.distributed.ONodeAddress;
import com.orientechnologies.orient.server.distributed.Record;
import com.orientechnologies.orient.server.distributed.operations.ODefaultDistributedCoordinatorFactory;
import com.orientechnologies.orient.server.distributed.ringprotocols.ODefaultRingProtocolsFactory;
import com.orientechnologies.orient.server.distributed.ringprotocols.OGlobalMaintenanceProtocolRunnable;
import com.orientechnologies.orient.server.distributed.ringprotocols.OLocalMaintenanceProtocolRunnable;

/**
 * @author Andrey Lomakin
 * @since 15.08.12
 */
public class ServerInstance implements MembershipListener, ODHTNodeLookup, LifecycleListener {
  public static final int                                 REPLICA_COUNT      = 2;
  private static final int                                SYNC_REPLICA_COUNT = 1;

  public static final Map<String, ServerInstance>         INSTANCES          = new ConcurrentHashMap<String, ServerInstance>();

  private final Map<ONodeAddress, OHazelcastDHTNodeProxy> addressProxyMap    = new ConcurrentHashMap<ONodeAddress, OHazelcastDHTNodeProxy>();
  private final Map<String, Member>                       uuidMemberMap      = new ConcurrentHashMap<String, Member>();

  private volatile OLocalDHTNode                          localNode;
  private volatile HazelcastInstance                      hazelcastInstance;
  private final Timer                                     timer              = new Timer("DHT timer", true);

  private final boolean                                   useReadRepair;
  private final boolean                                   useAntiEntropy;
  private final boolean                                   useGlobalMaintainence;
  private final int                                       replicaCount;
  private final int                                       syncReplicaCount;

  private final ScheduledExecutorService                  lmExecutorService  =
					Executors.newSingleThreadScheduledExecutor(new OLocalMaintenanceProtocolThreadFactory());

	private final ScheduledExecutorService                  gmExecutorService  =
					Executors.newSingleThreadScheduledExecutor(new OGlobalMaintenanceProtocolThreadFactory());


	public ServerInstance() {
    useReadRepair = true;
    useAntiEntropy = true;
    useGlobalMaintainence = true;

    replicaCount = REPLICA_COUNT;
    syncReplicaCount = SYNC_REPLICA_COUNT;
  }

  public ServerInstance(int replicaCount, int syncReplicaCount) {
    useReadRepair = true;
    useAntiEntropy = true;
    useGlobalMaintainence = true;

    this.replicaCount = replicaCount;
    this.syncReplicaCount = syncReplicaCount;
  }

  public ServerInstance(boolean useReadRepair, boolean useAntiEntropy, boolean useGlobalMaintainence) {
    this.useReadRepair = useReadRepair;
    this.useAntiEntropy = useAntiEntropy;
    this.useGlobalMaintainence = useGlobalMaintainence;

    replicaCount = REPLICA_COUNT;
    syncReplicaCount = SYNC_REPLICA_COUNT;
  }

  public void init() throws InterruptedException {
    XmlConfigBuilder xmlConfigBuilder = new XmlConfigBuilder(ServerInstance.class.getResourceAsStream("/hazelcast.xml"));

    hazelcastInstance = Hazelcast.newHazelcastInstance(xmlConfigBuilder.build());

    final ODefaultRingProtocolsFactory ringProtocolsFactory = new ODefaultRingProtocolsFactory(useReadRepair);
		final OHazelcastNodeAddress localNodeAddress =
						new OHazelcastNodeAddress(ONodeId.generateUniqueId(), hazelcastInstance.getCluster().getLocalMember().getUuid());

		localNode = new OLocalDHTNode(localNodeAddress, this, new ODefaultDistributedCoordinatorFactory(),
						ringProtocolsFactory,	replicaCount, syncReplicaCount);

    INSTANCES.put(hazelcastInstance.getCluster().getLocalMember().getUuid(), this);

    hazelcastInstance.getCluster().addMembershipListener(this);
    for (Member member : hazelcastInstance.getCluster().getMembers())
      uuidMemberMap.put(member.getUuid(), member);

    // TODO Fix concurrency issue here
    if (hazelcastInstance.getCluster().getMembers().size() == 1)
      localNode.createDHT();
    else {
      ONodeAddress nodeAddress;
      do {
        nodeAddress = peekAnyLocalNodeFromRemoteMember(hazelcastInstance.getCluster().getMembers().iterator().next());
      } while (!localNode.joinDHT(nodeAddress));
    }

    if (useAntiEntropy)
      lmExecutorService.scheduleWithFixedDelay(
							new OLocalMaintenanceProtocolRunnable(localNode,
											replicaCount, syncReplicaCount,
											ringProtocolsFactory.createLocalMaintenanceProtocol(this)),
							        1, 1, TimeUnit.SECONDS);

		if (useGlobalMaintainence)
			gmExecutorService.scheduleWithFixedDelay(
							new OGlobalMaintenanceProtocolRunnable(
											ringProtocolsFactory.createGlobalMaintenanceProtocol(this), localNode,
											replicaCount, syncReplicaCount),
							100, 100, TimeUnit.MILLISECONDS);

    timer.schedule(new TimerTask() {
      @Override
      public void run() {
        localNode.fixPredecessor();
        localNode.stabilize();
        localNode.fixFingers();
      }
    }, 10000, 10000);

  }

  public Record create(ORecordId id, String data) {
    return localNode.createRecord(id, data);
  }

  public Record create(String data) {
    return localNode.createRecord(data);
  }

  public Record get(ORecordId id) {
    return localNode.readRecord(id);
  }

  public void remove(ORecordId id, ODHTRecordVersion version) {
    localNode.deleteRecord(id, version);
  }

  public void memberAdded(MembershipEvent membershipEvent) {
    final Member member = membershipEvent.getMember();

    uuidMemberMap.put(member.getUuid(), member);
    localNode.stabilize();
  }

  public void memberRemoved(MembershipEvent membershipEvent) {
    final Member member = membershipEvent.getMember();

    uuidMemberMap.remove(member.getUuid());

    localNode.fixPredecessor();
    localNode.stabilize();
  }

  public ODHTNode findById(ONodeAddress address) {
    if (!uuidMemberMap.containsKey(((OHazelcastNodeAddress) address).getMemberUUID())) {
      addressProxyMap.remove(address);
      return null;
    }

    if (localNode.getNodeAddress().equals(address))
      return localNode;

    OHazelcastDHTNodeProxy dhtNodeProxy = addressProxyMap.get(address);
    if (dhtNodeProxy == null) {
      dhtNodeProxy = new OHazelcastDHTNodeProxy((OHazelcastNodeAddress) address, hazelcastInstance);
      addressProxyMap.put(address, dhtNodeProxy);
    }

    return dhtNodeProxy;
  }

  public boolean isRunning() {
    return hazelcastInstance.getLifecycleService().isRunning();
  }

  public int size() {
    return hazelcastInstance.getCluster().getMembers().size();
  }

  public OLocalDHTNode getLocalNode() {
    return localNode;
  }

  public ODHTNode findSuccessor(ONodeId id) {
    return findById(localNode.findSuccessor(id));
  }

  @Override
  public void stateChanged(LifecycleEvent event) {
  }

  public void shutdown() throws Exception {
    final String memberUUID = hazelcastInstance.getCluster().getLocalMember().getUuid();

    timer.cancel();

    lmExecutorService.shutdown();
		gmExecutorService.shutdown();

    if (!lmExecutorService.awaitTermination(180000, TimeUnit.MILLISECONDS))
      throw new IllegalStateException("LM service was not terminated.");

		if (!gmExecutorService.awaitTermination(180000, TimeUnit.MILLISECONDS))
			throw new IllegalStateException("GM service was not terminated.");

		hazelcastInstance.getLifecycleService().shutdown();
    INSTANCES.remove(memberUUID);
  }

  private ONodeAddress peekAnyLocalNodeFromRemoteMember(Member member) throws InterruptedException {

    final Future<ONodeAddress> future = (Future<ONodeAddress>) hazelcastInstance.getExecutorService().submit(
        new DistributedTask<ONodeAddress>(new PeekAnyLocalNode(member.getUuid()), member));
    try {
      return future.get();
    } catch (ExecutionException e) {
      return null;
    }
  }

  private static final class PeekAnyLocalNode implements Callable<ONodeAddress>, Externalizable {
    private String memberUUID;

    public PeekAnyLocalNode() {
    }

    private PeekAnyLocalNode(String memberUUID) {
      this.memberUUID = memberUUID;
    }

    @Override
    public ONodeAddress call() throws Exception {
      final ServerInstance serverInstance = ServerInstance.INSTANCES.get(memberUUID);

      return serverInstance.getLocalNode().getNodeAddress();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
      out.writeUTF(memberUUID);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
      memberUUID = in.readUTF();
    }
  }

	private static final class OGlobalMaintenanceProtocolThreadFactory implements ThreadFactory {
		@Override
		public Thread newThread(Runnable r) {
			final Thread thread = new Thread(r);
			thread.setDaemon(true);

			return thread;
		}
	}

	private static final class OLocalMaintenanceProtocolThreadFactory implements ThreadFactory {
		@Override
		public Thread newThread(Runnable r) {
			final Thread thread = new Thread(r);
			thread.setDaemon(true);

			return thread;
		}
	}

}
