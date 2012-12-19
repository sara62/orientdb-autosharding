package com.orientechnologies.orient.server.distributed.ringprotocols;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.id.ONodeId;
import com.orientechnologies.orient.server.distributed.ODHTNode;
import com.orientechnologies.orient.server.distributed.ODHTNodeLocal;
import com.orientechnologies.orient.server.distributed.ODHTNodeLookup;
import com.orientechnologies.orient.server.distributed.ONodeAddress;
import com.orientechnologies.orient.server.distributed.ORecordMetadata;
import com.orientechnologies.orient.server.distributed.merkletree.ODetachedMerkleTreeNode;
import com.orientechnologies.orient.server.distributed.merkletree.OMerkleTree;

/**
 * @author Andrey Lomakin
 * @since 30.11.12
 */
@Test
public class LocalMaintenanceProtocolImplTest {
	private OLocalMaintenanceProtocolImpl localMaintenanceProtocol;

	@Mock
	private ODHTNodeLocal localNode;

	@Mock
	private OReplicaDistributionStrategy replicaDistributionStrategy;

	@Mock
	private ODHTNodeLookup nodeLookup;

	@Mock
	private OMerkleTree merkleTree;

	@Mock
	private OLocalMaintenanceProtocolNodeComparator nodeComparator;

	@BeforeClass
	public void beforeClass() {
		OGlobalConfiguration.DB_USE_DISTRIBUTED_VERSION.setValue(true);
	}


	@BeforeMethod
	public void setUp() {
		MockitoAnnotations.initMocks(this);

		localMaintenanceProtocol = new OLocalMaintenanceProtocolImpl(replicaDistributionStrategy, nodeLookup,
						new OLocalMaintenanceProtocolNodeComparator[]{nodeComparator});

	}

	public void testPredecessorIsNull() {
		when(localNode.getPredecessor()).thenReturn(null);
		when(localNode.state()).thenReturn(ODHTNode.NodeState.PRODUCTION);

		localMaintenanceProtocol.synchronizeReplicasBetweenHolders(localNode, storageName, clusterId, 2, 1);

		verify(localNode).getPredecessor();
		verify(localNode).state();

		verifyZeroInteractions(replicaDistributionStrategy, nodeLookup);
		verifyNoMoreInteractions(localNode);
	}

	public void testRootsAreLeavesAndEqual() {
		final ONodeAddress localNodeAddress = new ONodeAddressStub(ONodeId.generateUniqueId());
		final ONodeAddress predecessorAddress = new ONodeAddressStub(localNodeAddress.getNodeId().subtract(ONodeId.valueOf(10000)));

		when(localNode.getNodeAddress()).thenReturn(localNodeAddress);
		when(localNode.getPredecessor()).thenReturn(predecessorAddress);
		when(localNode.state()).thenReturn(ODHTNode.NodeState.PRODUCTION);

		final ONodeAddress replicaHolderOne = new ONodeAddressStub(localNodeAddress.getNodeId().add(ONodeId.valueOf(1000)));
		final ONodeAddress replicaHolderTwo = new ONodeAddressStub(replicaHolderOne.getNodeId().add(ONodeId.valueOf(1000)));
		final ONodeAddress replicaHolderThree = new ONodeAddressStub(replicaHolderTwo.getNodeId().add(ONodeId.valueOf(1000)));

		when(localNode.getSuccessors()).thenReturn(new ONodeAddress[]{replicaHolderOne, replicaHolderTwo, replicaHolderThree});

		when(
						replicaDistributionStrategy.chooseReplicas(new ONodeAddress[]{replicaHolderOne, replicaHolderTwo, replicaHolderThree},
										2, 1)).thenReturn(
						new Set[]{new HashSet(Arrays.asList(replicaHolderOne)), new HashSet(Arrays.asList(replicaHolderTwo))});

		final Random random = new Random();
		final byte[] hash = new byte[256];
		random.nextBytes(hash);

		final ORecordMetadata[] recordMetadata = new ORecordMetadata[64];

		final ODetachedMerkleTreeNode localRootNode = new ODetachedMerkleTreeNode(hash,
						predecessorAddress.getNodeId().add(ONodeId.ONE), localNodeAddress.getNodeId(), recordMetadata, null, new int[0]);

		final ODetachedMerkleTreeNode remoteRootNode = new ODetachedMerkleTreeNode(hash, predecessorAddress.getNodeId()
						.add(ONodeId.ONE), localNodeAddress.getNodeId(), null, null, new int[0]);

		when(localNode.getLocalMerkleTree(null, -1)).thenReturn(merkleTree);
		when(merkleTree.getRootNodesForInterval(predecessorAddress.getNodeId().add(ONodeId.ONE), localNodeAddress.getNodeId()))
						.thenReturn(Arrays.asList(localRootNode));

		final ODHTNode remoteNodeOne = mock(ODHTNode.class);
		final ODHTNode remoteNodeTwo = mock(ODHTNode.class);

		when(nodeLookup.findById(replicaHolderOne)).thenReturn(remoteNodeOne);
		when(nodeLookup.findById(replicaHolderTwo)).thenReturn(remoteNodeTwo);

		when(remoteNodeOne.findMerkleTreeNode(null, localRootNode)).thenReturn(remoteRootNode);
		when(remoteNodeTwo.findMerkleTreeNode(null, localRootNode)).thenReturn(remoteRootNode);

		localMaintenanceProtocol.synchronizeReplicasBetweenHolders(localNode, storageName, clusterId, 2, 1);

		verifyZeroInteractions(nodeComparator);
	}

	public void testVerifyOneChildInsideRootIntervalAndSecondIsNot() {
		final CreateTwoDifferentNodesWithOnlyOneInside processFlow = new CreateTwoDifferentNodesWithOnlyOneInside();
		localMaintenanceProtocol.synchronizeReplicasBetweenHolders(localNode, storageName, clusterId, 2, 1);

		verify(nodeComparator).compareNodes(storageName, localNode, processFlow.localRootNode, processFlow.remoteRootNode,
						processFlow.replicaHolderOne);

		verify(nodeComparator).compareNodes(storageName, localNode, processFlow.localRootNode, processFlow.remoteRootNode,
						processFlow.replicaHolderTwo);

		verify(nodeComparator).compareNodes(storageName, localNode, processFlow.firstLocalChildNode, processFlow.firstRemoteChildNode,
						processFlow.replicaHolderOne);

		verify(nodeComparator).compareNodes(storageName, localNode, processFlow.firstLocalChildNode, processFlow.firstRemoteChildNode,
						processFlow.replicaHolderTwo);

		verifyNoMoreInteractions(nodeComparator);
	}

	public void testOneOfTheServerNodesThrowsExceptionDuringProcessing() {
		final OneReplicaHolderThrowsExceptionDuringProcessing processFlow = new OneReplicaHolderThrowsExceptionDuringProcessing();

		localMaintenanceProtocol.synchronizeReplicasBetweenHolders(localNode, storageName, clusterId, 2, 1);

		verify(nodeComparator).compareNodes(storageName, localNode, processFlow.localRootNode, processFlow.remoteRootNode,
						processFlow.replicaHolderOne);

		verify(nodeComparator).compareNodes(storageName, localNode, processFlow.localRootNode, processFlow.remoteRootNode,
						processFlow.replicaHolderTwo);

		verify(nodeComparator).compareNodes(storageName, localNode, processFlow.firstLocalChildNode, processFlow.firstRemoteChildNode,
						processFlow.replicaHolderOne);

		verifyNoMoreInteractions(nodeComparator);
	}

	public void testOneOfTheServerNodesIsOffline() {
		final OneReplicaHolderIsOffline processFlow = new OneReplicaHolderIsOffline();

		localMaintenanceProtocol.synchronizeReplicasBetweenHolders(localNode, storageName, clusterId, 2, 1);

		verify(nodeComparator).compareNodes(storageName, localNode, processFlow.localRootNode, processFlow.remoteRootNode,
						processFlow.replicaHolderOne);

		verify(nodeComparator).compareNodes(storageName, localNode, processFlow.firstLocalChildNode, processFlow.firstRemoteChildNode,
						processFlow.replicaHolderOne);

		verifyNoMoreInteractions(nodeComparator);
	}

	public void testPredecessorIsNullSecondTime() {
		final PredecessorIsNullSecondTime processFlow = new PredecessorIsNullSecondTime();

		localMaintenanceProtocol.synchronizeReplicasBetweenHolders(localNode, storageName, clusterId, 2, 1);

		verifyZeroInteractions(nodeComparator);
	}

	public void testRemoteTreeNodeIsNull() {
		RemoteTreeNodeIsNull processFlow = new RemoteTreeNodeIsNull();

		localMaintenanceProtocol.synchronizeReplicasBetweenHolders(localNode, storageName, clusterId, 2, 1);

		verify(nodeComparator).compareNodes(storageName, localNode, processFlow.localRootNode, processFlow.remoteRootNode,
						processFlow.replicaHolderOne);

		verify(nodeComparator).compareNodes(storageName, localNode, processFlow.localRootNode, processFlow.remoteRootNode,
						processFlow.replicaHolderTwo);

		verify(nodeComparator).compareNodes(storageName, localNode, processFlow.firstLocalChildNode, processFlow.firstRemoteChildNode,
						processFlow.replicaHolderOne);

		verifyNoMoreInteractions(nodeComparator);
	}

	public void testLocalTreeChildNodeIsNull() {
		LocalTreeNodeIsNull processFlow = new LocalTreeNodeIsNull();

		localMaintenanceProtocol.synchronizeReplicasBetweenHolders(localNode, storageName, clusterId, 2, 1);

		verify(nodeComparator).compareNodes(storageName, localNode, processFlow.localRootNode, processFlow.remoteRootNode,
						processFlow.replicaHolderOne);

		verify(nodeComparator).compareNodes(storageName, localNode, processFlow.localRootNode, processFlow.remoteRootNode,
						processFlow.replicaHolderTwo);

		verifyNoMoreInteractions(nodeComparator);
	}

	public void testRootNodesAreDifferentRemoteIsLeafButLocalIsNot() {
		RootNodesAreDifferentRemoteIsLeafButLocalIsNot processFlow = new RootNodesAreDifferentRemoteIsLeafButLocalIsNot();

		localMaintenanceProtocol.synchronizeReplicasBetweenHolders(localNode, storageName, clusterId, 2, 1);

		verify(nodeComparator).compareNodes(storageName, localNode, processFlow.localRootNode, processFlow.remoteRootNode,
						processFlow.replicaHolderOne);

		verify(nodeComparator).compareNodes(storageName, localNode, processFlow.localRootNode, processFlow.remoteRootNode,
						processFlow.replicaHolderTwo);

		verifyNoMoreInteractions(nodeComparator);
	}


	public void testRootNodesAreDifferentLocalIsLeafButRemoteIsNot() {
		RootNodesAreDifferentLocalIsLeafButRemoteIsNot processFlow = new RootNodesAreDifferentLocalIsLeafButRemoteIsNot();

		localMaintenanceProtocol.synchronizeReplicasBetweenHolders(localNode, storageName, clusterId, 2, 1);

		verify(nodeComparator).compareNodes(storageName, localNode, processFlow.localRootNode, processFlow.remoteRootNode,
						processFlow.replicaHolderOne);

		verify(nodeComparator).compareNodes(storageName, localNode, processFlow.localRootNode, processFlow.remoteRootNode,
						processFlow.replicaHolderTwo);

		verifyNoMoreInteractions(nodeComparator);
	}

	public void testLeafChildIsNotEqualOnThirdLevel() {
		LeafChildIsNotEqualOnThirdLevel processFlow = new LeafChildIsNotEqualOnThirdLevel();

		localMaintenanceProtocol.synchronizeReplicasBetweenHolders(localNode, storageName, clusterId, 2, 1);

		verify(nodeComparator).compareNodes(storageName, localNode, processFlow.localRootNode, processFlow.remoteRootNode,
						processFlow.replicaHolderOne);
		verify(nodeComparator).compareNodes(storageName, localNode, processFlow.localRootNode, processFlow.remoteRootNode,
						processFlow.replicaHolderTwo);

		verify(nodeComparator).compareNodes(storageName, localNode, processFlow.firstLocalChildNode, processFlow.firstRemoteChildNode,
						processFlow.replicaHolderOne);
		verify(nodeComparator).compareNodes(storageName, localNode, processFlow.firstLocalChildNode, processFlow.firstRemoteChildNode,
						processFlow.replicaHolderTwo);

		verify(nodeComparator).compareNodes(storageName, localNode, processFlow.firstLocalChildNodeSecondLevel,
						processFlow.firstRemoteChildNodeSecondLevel,
						processFlow.replicaHolderOne);
		verify(nodeComparator).compareNodes(storageName, localNode, processFlow.firstLocalChildNodeSecondLevel,
						processFlow.firstRemoteChildNodeSecondLevel,
						processFlow.replicaHolderTwo);

		verifyNoMoreInteractions(nodeComparator);
	}

	public void testPredecessorIsGreaterThanNodeIdSecondIntervalIsSameAsFirst() {
		PredecessorIsGreaterThanNodeIdSecondIntervalIsSameAsFirst processFlow =
						new PredecessorIsGreaterThanNodeIdSecondIntervalIsSameAsFirst();

		localMaintenanceProtocol.synchronizeReplicasBetweenHolders(localNode, storageName, clusterId, 2, 1);

		verify(nodeComparator).compareNodes(storageName, localNode, processFlow.localRootNode, processFlow.remoteRootNode,
						processFlow.replicaHolderOne);
		verify(nodeComparator).compareNodes(storageName, localNode, processFlow.localRootNode, processFlow.remoteRootNode,
						processFlow.replicaHolderTwo);

		verify(nodeComparator).compareNodes(storageName, localNode, processFlow.firstLocalChildNode, processFlow.firstRemoteChildNode,
						processFlow.replicaHolderOne);
		verify(nodeComparator).compareNodes(storageName, localNode, processFlow.firstLocalChildNode, processFlow.firstRemoteChildNode,
						processFlow.replicaHolderTwo);

		verify(nodeComparator).compareNodes(storageName, localNode, processFlow.secondLocalChildNode, processFlow.secondRemoteChildNode,
						processFlow.replicaHolderOne);
		verify(nodeComparator).compareNodes(storageName, localNode, processFlow.secondLocalChildNode, processFlow.secondRemoteChildNode,
						processFlow.replicaHolderTwo);


		verifyNoMoreInteractions(nodeComparator);
	}

	public void testPredecessorIsGreaterThanNodeIdSecondIntervalIsNotSameAsFirst() {
		PredecessorIsGreaterThanNodeIdSecondIntervalIsNotSameAsFirst processFlow =
						new PredecessorIsGreaterThanNodeIdSecondIntervalIsNotSameAsFirst();

		localMaintenanceProtocol.synchronizeReplicasBetweenHolders(localNode, storageName, clusterId, 2, 1);

		verify(nodeComparator).compareNodes(storageName, localNode, processFlow.localRootNode, processFlow.remoteRootNode,
						processFlow.replicaHolderOne);
		verify(nodeComparator).compareNodes(storageName, localNode, processFlow.localRootNode, processFlow.remoteRootNode,
						processFlow.replicaHolderTwo);

		verify(nodeComparator).compareNodes(storageName, localNode, processFlow.firstLocalChildNode, processFlow.firstRemoteChildNode,
						processFlow.replicaHolderOne);
		verify(nodeComparator).compareNodes(storageName, localNode, processFlow.firstLocalChildNode, processFlow.firstRemoteChildNode,
						processFlow.replicaHolderTwo);

		verify(nodeComparator).compareNodes(storageName, localNode, processFlow.secondLocalRootNode, processFlow.secondRemoteRootNode,
						processFlow.replicaHolderOne);
		verify(nodeComparator).compareNodes(storageName, localNode, processFlow.secondLocalRootNode, processFlow.secondRemoteRootNode,
						processFlow.replicaHolderTwo);


		verifyNoMoreInteractions(nodeComparator);
	}

	public void testPredecessorIsGreaterThanNodeIdThreeRootNodesOneIsTheSameOne() {
		PredecessorIsGreaterThanNodeIdThreeRootNodesOneIsTheSameOne processFlow =
						new PredecessorIsGreaterThanNodeIdThreeRootNodesOneIsTheSameOne();

		localMaintenanceProtocol.synchronizeReplicasBetweenHolders(localNode, storageName, clusterId, 2, 1);

		verify(nodeComparator).compareNodes(storageName, localNode, processFlow.localRootNode, processFlow.remoteRootNode,
						processFlow.replicaHolderOne);
		verify(nodeComparator).compareNodes(storageName, localNode, processFlow.localRootNode, processFlow.remoteRootNode,
						processFlow.replicaHolderTwo);

		verify(nodeComparator).compareNodes(storageName, localNode, processFlow.firstLocalChildNode, processFlow.firstRemoteChildNode,
						processFlow.replicaHolderOne);
		verify(nodeComparator).compareNodes(storageName, localNode, processFlow.firstLocalChildNode, processFlow.firstRemoteChildNode,
						processFlow.replicaHolderTwo);

		verify(nodeComparator).compareNodes(storageName, localNode, processFlow.secondLocalRootNode, processFlow.secondRemoteRootNode,
						processFlow.replicaHolderOne);
		verify(nodeComparator).compareNodes(storageName, localNode, processFlow.secondLocalRootNode, processFlow.secondRemoteRootNode,
						processFlow.replicaHolderTwo);


		verifyNoMoreInteractions(nodeComparator);
	}

	public void testPredecessorIsGreaterThanNodeIdThreeRootNodesOneIsTheSameTwo() {
		PredecessorIsGreaterThanNodeIdThreeRootNodesOneIsTheSameTwo processFlow =
						new PredecessorIsGreaterThanNodeIdThreeRootNodesOneIsTheSameTwo();

		localMaintenanceProtocol.synchronizeReplicasBetweenHolders(localNode, storageName, clusterId, 2, 1);

		verify(nodeComparator).compareNodes(storageName, localNode, processFlow.localRootNode, processFlow.remoteRootNode,
						processFlow.replicaHolderOne);
		verify(nodeComparator).compareNodes(storageName, localNode, processFlow.localRootNode, processFlow.remoteRootNode,
						processFlow.replicaHolderTwo);

		verify(nodeComparator).compareNodes(storageName, localNode, processFlow.firstLocalChildNode, processFlow.firstRemoteChildNode,
						processFlow.replicaHolderOne);
		verify(nodeComparator).compareNodes(storageName, localNode, processFlow.firstLocalChildNode, processFlow.firstRemoteChildNode,
						processFlow.replicaHolderTwo);

		verify(nodeComparator).compareNodes(storageName, localNode, processFlow.secondLocalRootNode, processFlow.secondRemoteRootNode,
						processFlow.replicaHolderOne);
		verify(nodeComparator).compareNodes(storageName, localNode, processFlow.secondLocalRootNode, processFlow.secondRemoteRootNode,
						processFlow.replicaHolderTwo);


		verifyNoMoreInteractions(nodeComparator);
	}


	private class LocalTreeNodeIsNull extends CreateTwoDifferentNodesWithOnlyOneInside {
		private LocalTreeNodeIsNull() {
			when(merkleTree.getChildNode(localRootNode, 0)).thenReturn(null);
		}
	}

	private class RemoteTreeNodeIsNull extends CreateTwoDifferentNodesWithOnlyOneInside {
		private RemoteTreeNodeIsNull() {
			when(remoteNodeTwo.findMerkleTreeNode(null, firstLocalChildNode)).thenReturn(null);
		}
	}

	private class PredecessorIsGreaterThanNodeIdThreeRootNodesOneIsTheSameTwo extends
					PredecessorIsGreaterThanNodeIdSecondIntervalIsNotSameAsFirst {

		private PredecessorIsGreaterThanNodeIdThreeRootNodesOneIsTheSameTwo() {
			when(merkleTree.getRootNodesForInterval(ONodeId.ZERO, localNodeAddress().getNodeId())).
							thenReturn(Arrays.asList(localRootNode));
			when(merkleTree.getRootNodesForInterval(predecessorAddress().getNodeId().add(ONodeId.ONE), ONodeId.MAX_VALUE)).
							thenReturn(Arrays.asList(localRootNode, secondLocalRootNode));
		}
	}

	private class PredecessorIsGreaterThanNodeIdThreeRootNodesOneIsTheSameOne extends
					PredecessorIsGreaterThanNodeIdSecondIntervalIsNotSameAsFirst {

		private PredecessorIsGreaterThanNodeIdThreeRootNodesOneIsTheSameOne() {
			when(merkleTree.getRootNodesForInterval(ONodeId.ZERO, localNodeAddress().getNodeId())).
							thenReturn(Arrays.asList(localRootNode, secondLocalRootNode));
			when(merkleTree.getRootNodesForInterval(predecessorAddress().getNodeId().add(ONodeId.ONE), ONodeId.MAX_VALUE)).
							thenReturn(Arrays.asList(secondLocalRootNode));
		}
	}

	private class PredecessorIsGreaterThanNodeIdSecondIntervalIsNotSameAsFirst extends
					PredecessorIsGreaterThanNodeIdSecondIntervalIsSameAsFirst {

		public 	ODetachedMerkleTreeNode secondLocalRootNode;
		public  ODetachedMerkleTreeNode secondRemoteRootNode;

		private PredecessorIsGreaterThanNodeIdSecondIntervalIsNotSameAsFirst() {
			final byte[] secondRootLocalHash = new byte[256];

			final Random random = new Random();
			random.nextBytes(secondRootLocalHash);

			final byte[] secondRootRemoteHash = Arrays.copyOf(secondRootLocalHash, 256);
			secondRootRemoteHash[0] = (byte)(secondRootLocalHash[0] + 1);

			secondLocalRootNode = new ODetachedMerkleTreeNode(secondRootLocalHash,
							predecessorAddress().getNodeId().add(ONodeId.ONE), ONodeId.MAX_VALUE, new ORecordMetadata[0],
							null, new int[0]);

			secondRemoteRootNode = new ODetachedMerkleTreeNode(secondRootRemoteHash,
							predecessorAddress().getNodeId().add(ONodeId.ONE), ONodeId.MAX_VALUE, new ORecordMetadata[0],
							null, new int[0]);

			when(merkleTree.getRootNodesForInterval(predecessorAddress().getNodeId().add(ONodeId.ONE), ONodeId.MAX_VALUE)).
							thenReturn(Arrays.asList(localRootNode));
			when(merkleTree.getRootNodesForInterval(ONodeId.ZERO, localNodeAddress().getNodeId())).
							thenReturn(Arrays.asList(secondLocalRootNode));


			when(remoteNodeOne.findMerkleTreeNode(null, secondLocalRootNode)).thenReturn(secondRemoteRootNode);
			when(remoteNodeTwo.findMerkleTreeNode(null, secondLocalRootNode)).thenReturn(secondRemoteRootNode);
		}

		@Override
		protected ONodeId localRootNodeStartId() {
			return ONodeId.ZERO;
		}

		@Override
		protected ONodeId localRootNodeEndId() {
			return predecessorAddress().getNodeId();
		}

		@Override
		protected ONodeId remoteRootNodeEndId() {
			return localRootNodeEndId();
		}

		@Override
		protected ONodeId remoteRootNodeStartId() {
			return localRootNodeStartId();
		}

		@Override
		protected ONodeId secondRemoteChildNodeEndId() {
			return secondLocalChildNodeEndId();
		}

		@Override
		protected ONodeId secondRemoteChildNodeStartId() {
			return secondLocalChildNodeStartId();
		}

		@Override
		protected ONodeId secondLocalChildNodeEndId() {
			return predecessorAddress().getNodeId();
		}

		@Override
		protected ONodeId secondLocalChildNodeStartId() {
			return localNodeAddress().getNodeId().add(ONodeId.ONE);
		}
	}

	private class PredecessorIsGreaterThanNodeIdSecondIntervalIsSameAsFirst extends CreateTwoDifferentNodesWithOnlyOneInside {
		private PredecessorIsGreaterThanNodeIdSecondIntervalIsSameAsFirst() {
			when(merkleTree.getRootNodesForInterval(predecessorAddress().getNodeId().add(ONodeId.ONE), ONodeId.MAX_VALUE)).
							thenReturn(Arrays.asList(localRootNode));
			when(merkleTree.getRootNodesForInterval(ONodeId.ZERO, localNodeAddress().getNodeId())).
							thenReturn(Arrays.asList(localRootNode));
		}

		@Override
		protected ONodeAddressStub localNodeAddress() {
			return new ONodeAddressStub(ONodeId.valueOf(1000));
		}

		@Override
		protected ONodeAddressStub predecessorAddress() {
			return new ONodeAddressStub(ONodeId.valueOf(10000));
		}

		@Override
		protected ONodeId secondRemoteChildNodeEndId() {
			return secondLocalChildNodeEndId();
		}

		@Override
		protected ONodeId secondRemoteChildNodeStartId() {
			return secondLocalChildNodeStartId();
		}

		@Override
		protected ONodeId secondLocalChildNodeEndId() {
			return ONodeId.MAX_VALUE;
		}

		@Override
		protected ONodeId secondLocalChildNodeStartId() {
			return localNodeAddress().getNodeId().subtract(ONodeId.ONE);
		}

		@Override
		protected ONodeId firstRemoteChildNodeStartId() {
			return localChildNodeStartId();
		}

		@Override
		protected ONodeId firstRemoteChildNodeEndId() {
			return localChildNodeEndId();
		}

		@Override
		protected ONodeId firstLocalChildNodeEndId() {
			return localNodeAddress().getNodeId().subtract(ONodeId.TWO);
		}

		@Override
		protected ONodeId firstLocalChildNodeStartId() {
			return ONodeId.ZERO;
		}

		@Override
		protected ONodeId remoteChildNodeEndId() {
			return localChildNodeEndId();
		}

		@Override
		protected ONodeId remoteChildNodeStartId() {
			return localChildNodeStartId();
		}

		@Override
		protected ONodeId localChildNodeEndId() {
			return localNodeAddress().getNodeId().subtract(ONodeId.TWO);
		}

		@Override
		protected ONodeId localChildNodeStartId() {
			return ONodeId.ZERO;
		}

		@Override
		protected ONodeId remoteRootNodeStartId() {
			return ONodeId.ZERO;
		}

		@Override
		protected ONodeId remoteRootNodeEndId() {
			return ONodeId.MAX_VALUE;
		}

		@Override
		protected ONodeId localRootNodeEndId() {
			return ONodeId.MAX_VALUE;
		}

		@Override
		protected ONodeId localRootNodeStartId() {
			return ONodeId.ZERO;
		}
	}

	private class LeafChildIsNotEqualOnThirdLevel extends CreateTwoDifferentNodesWithOnlyOneInside {

		public ODetachedMerkleTreeNode firstLocalChildNodeSecondLevel;
		public ODetachedMerkleTreeNode firstRemoteChildNodeSecondLevel;

		private LeafChildIsNotEqualOnThirdLevel() {
			final Random random = new Random();

			final byte[] localRootHash = new byte[256];
			random.nextBytes(localRootHash);

			final byte[] remoteRootHash = Arrays.copyOf(localRootHash, 256);
			remoteRootHash[0] = (byte) (remoteRootHash[0] + 1);

			final byte[] childHash = new byte[256];
			random.nextBytes(childHash);

			final byte[] firstLocalDiffChildHash = new byte[256];
			random.nextBytes(firstLocalDiffChildHash);

			final byte[] firstRemoteDiffChildHash = Arrays.copyOf(firstLocalDiffChildHash, 256);
			firstRemoteDiffChildHash[0] = (byte) (firstLocalDiffChildHash[0] + 1);

			final byte[] secondLocalDiffChildHash = new byte[256];
			random.nextBytes(secondLocalDiffChildHash);

			final byte[] secondRemoteDiffChildHash = Arrays.copyOf(secondLocalDiffChildHash, 256);
			secondLocalDiffChildHash[0] = (byte) (secondRemoteDiffChildHash[0] + 1);

			final ORecordMetadata[] recordMetadata = new ORecordMetadata[64];

			final byte[][] localChildrenHash = new byte[64][];
			for (int i = 0; i < 64; i++) {
				if (i == 2)
					localChildrenHash[i] = firstLocalDiffChildHash;
				else if (i == 63)
					localChildrenHash[i] = secondLocalDiffChildHash;
				else
					localChildrenHash[i] = childHash;
			}

			final byte[][] remoteChildrenHash = new byte[64][];
			for (int i = 0; i < 64; i++) {
				if (i == 2)
					localChildrenHash[i] = firstRemoteDiffChildHash;
				else if (i == 63)
					localChildrenHash[i] = secondRemoteDiffChildHash;
				else
					localChildrenHash[i] = childHash;
			}

			final byte[] childHashThirdLevel = new byte[256];
			random.nextBytes(childHashThirdLevel);

			final byte[] childHashDiffLocalThirdLevel = new byte[256];
			random.nextBytes(childHashDiffLocalThirdLevel);

			final byte[] childHashDiffRemoteThirdLevel = Arrays.copyOf(childHashDiffLocalThirdLevel, 256);
			childHashDiffRemoteThirdLevel[0] = (byte) (childHashDiffLocalThirdLevel[0] + 1);

			final byte[][] localChildrenHashSecondLevel = new byte[64][];
			for (int i = 0; i < 64; i++) {
				if (i == 0)
					localChildrenHashSecondLevel[i] = childHashDiffLocalThirdLevel;
				else
					localChildrenHashSecondLevel[i] = childHashThirdLevel;
			}

			final byte[][] remoteChildrenHashSecondLevel = new byte[64][];
			for (int i = 0; i < 64; i++) {
				if (i == 0)
					remoteChildrenHashSecondLevel[i] = childHashDiffRemoteThirdLevel;
				else
					remoteChildrenHashSecondLevel[i] = childHashThirdLevel;
			}

			final ONodeAddress localNodeAddress = localNode.getNodeAddress();

			localRootNode = new ODetachedMerkleTreeNode(localRootHash, predecessorAddress.getNodeId().add(ONodeId.ONE),
							localNodeAddress.getNodeId(), null, localChildrenHash, new int[0]);

			remoteRootNode = new ODetachedMerkleTreeNode(remoteRootHash, predecessorAddress.getNodeId().add(ONodeId.ONE),
							localNodeAddress.getNodeId(), null, remoteChildrenHash, new int[0]);

			final ODetachedMerkleTreeNode localChildNode = new ODetachedMerkleTreeNode(childHash, predecessorAddress.getNodeId().add(
							ONodeId.ONE), localNodeAddress.getNodeId().subtract(ONodeId.TWO), recordMetadata, null, new int[0]);

			final ODetachedMerkleTreeNode remoteChildNode = new ODetachedMerkleTreeNode(childHash, predecessorAddress.getNodeId().add(
							ONodeId.ONE), localNodeAddress.getNodeId().subtract(ONodeId.TWO), recordMetadata, null, new int[0]);

			firstLocalChildNode = new ODetachedMerkleTreeNode(firstLocalDiffChildHash, predecessorAddress.getNodeId().add(ONodeId.ONE),
							localNodeAddress.getNodeId().subtract(ONodeId.TWO), null, localChildrenHashSecondLevel, new int[0]);

			firstRemoteChildNode = new ODetachedMerkleTreeNode(firstRemoteDiffChildHash, predecessorAddress.getNodeId().add(ONodeId.ONE),
							localNodeAddress.getNodeId().subtract(ONodeId.TWO), null, remoteChildrenHashSecondLevel, new int[0]);

			final ODetachedMerkleTreeNode secondLocalChildNode = new ODetachedMerkleTreeNode(secondLocalDiffChildHash, localNodeAddress
							.getNodeId().add(ONodeId.ONE), localNodeAddress.getNodeId().add(ONodeId.TWO), recordMetadata, null, new int[0]);

			final ODetachedMerkleTreeNode secondRemoteChildNode = new ODetachedMerkleTreeNode(secondRemoteDiffChildHash, localNodeAddress
							.getNodeId().add(ONodeId.ONE), localNodeAddress.getNodeId().add(ONodeId.TWO), recordMetadata, null, new int[0]);

			firstLocalChildNodeSecondLevel =
							new ODetachedMerkleTreeNode(childHashDiffLocalThirdLevel, predecessorAddress.getNodeId().add(ONodeId.ONE),
											localNodeAddress.getNodeId().subtract(ONodeId.TWO), recordMetadata, null, new int[0]);

			firstRemoteChildNodeSecondLevel =
							new ODetachedMerkleTreeNode(childHashDiffRemoteThirdLevel, predecessorAddress.getNodeId().add(ONodeId.ONE),
											localNodeAddress.getNodeId().subtract(ONodeId.TWO), recordMetadata, null, new int[0]);

			final ODetachedMerkleTreeNode localChildNodeSecondLevel =
							new ODetachedMerkleTreeNode(childHashThirdLevel, predecessorAddress.getNodeId().add(ONodeId.ONE),
											localNodeAddress.getNodeId().subtract(ONodeId.TWO), recordMetadata, null, new int[0]);

			final ODetachedMerkleTreeNode remoteChildNodeSecondLevel =
							new ODetachedMerkleTreeNode(childHashThirdLevel, predecessorAddress.getNodeId().add(ONodeId.ONE),
											localNodeAddress.getNodeId().subtract(ONodeId.TWO), recordMetadata, null, new int[0]);


			when(localNode.getLocalMerkleTree(null, -1)).thenReturn(merkleTree);
			when(merkleTree.getRootNodesForInterval(predecessorAddress.getNodeId().add(ONodeId.ONE), localNodeAddress.getNodeId()))
							.thenReturn(Arrays.asList(localRootNode));

			final ODHTNode remoteNodeOne = mock(ODHTNode.class);

			remoteNodeTwo = mock(ODHTNode.class);

			when(nodeLookup.findById(replicaHolderOne)).thenReturn(remoteNodeOne);
			when(nodeLookup.findById(replicaHolderTwo)).thenReturn(remoteNodeTwo);

			when(remoteNodeOne.findMerkleTreeNode(null, localRootNode)).thenReturn(remoteRootNode);
			when(remoteNodeTwo.findMerkleTreeNode(null, localRootNode)).thenReturn(remoteRootNode);

			for (int i = 0; i < 64; i++) {
				if (i == 2) {
					when(merkleTree.getChildNode(localRootNode, i)).thenReturn(firstLocalChildNode);
					when(remoteNodeOne.findMerkleTreeNode(null, firstLocalChildNode)).thenReturn(firstRemoteChildNode);
					when(remoteNodeTwo.findMerkleTreeNode(null, firstLocalChildNode)).thenReturn(firstRemoteChildNode);
				} else if (i == 63) {
					when(merkleTree.getChildNode(localRootNode, i)).thenReturn(secondLocalChildNode);
					when(remoteNodeOne.findMerkleTreeNode(null, secondLocalChildNode)).thenReturn(secondRemoteChildNode);
					when(remoteNodeTwo.findMerkleTreeNode(null, secondLocalChildNode)).thenReturn(secondRemoteChildNode);
				} else {
					when(merkleTree.getChildNode(localRootNode, i)).thenReturn(localChildNode);
					when(remoteNodeOne.findMerkleTreeNode(null, localChildNode)).thenReturn(remoteChildNode);
					when(remoteNodeTwo.findMerkleTreeNode(null, localChildNode)).thenReturn(remoteChildNode);
				}
			}

			for (int i = 0; i < 64; i++) {
				if (i == 0) {
					when(merkleTree.getChildNode(firstLocalChildNode, i)).thenReturn(firstLocalChildNodeSecondLevel);
					when(remoteNodeOne.findMerkleTreeNode(null, firstLocalChildNodeSecondLevel)).thenReturn(firstRemoteChildNodeSecondLevel);
					when(remoteNodeTwo.findMerkleTreeNode(null, firstLocalChildNodeSecondLevel)).thenReturn(firstRemoteChildNodeSecondLevel);
				} else {
					when(merkleTree.getChildNode(firstLocalChildNode, i)).thenReturn(localChildNodeSecondLevel);
					when(remoteNodeOne.findMerkleTreeNode(null, localChildNodeSecondLevel)).thenReturn(remoteChildNodeSecondLevel);
					when(remoteNodeTwo.findMerkleTreeNode(null, localChildNodeSecondLevel)).thenReturn(remoteChildNodeSecondLevel);
				}
			}

		}
	}

	private class RootNodesAreDifferentLocalIsLeafButRemoteIsNot extends CreateTwoDifferentNodesWithOnlyOneInside {
		private RootNodesAreDifferentLocalIsLeafButRemoteIsNot() {
			final Random random = new Random();

			final byte[] localRootHash = new byte[256];
			random.nextBytes(localRootHash);

			final byte[] remoteRootHash = Arrays.copyOf(localRootHash, 256);
			remoteRootHash[0] = (byte) (remoteRootHash[0] + 1);

			final byte[] childHash = new byte[256];
			random.nextBytes(childHash);

			final byte[] firstRemoteDiffChildHash = new byte[256];
			random.nextBytes(firstRemoteDiffChildHash);

			final byte[] secondRemoteDiffChildHash = new byte[256];
			random.nextBytes(secondRemoteDiffChildHash);

			final ORecordMetadata[] recordMetadata = new ORecordMetadata[64];

			final byte[][] remoteChildHash = new byte[64][];
			for (int i = 0; i < 64; i++) {
				if (i == 2)
					remoteChildHash[i] = firstRemoteDiffChildHash;
				else if (i == 63)
					remoteChildHash[i] = secondRemoteDiffChildHash;
				else
					remoteChildHash[i] = childHash;
			}

			final ONodeAddress localNodeAddress = localNode.getNodeAddress();

			remoteRootNode = new ODetachedMerkleTreeNode(localRootHash, predecessorAddress.getNodeId().add(ONodeId.ONE),
							localNodeAddress.getNodeId(), null, remoteChildHash, new int[0]);

			localRootNode = new ODetachedMerkleTreeNode(remoteRootHash, predecessorAddress.getNodeId().add(ONodeId.ONE),
							localNodeAddress.getNodeId(), recordMetadata, null, new int[0]);

			when(localNode.getLocalMerkleTree(null, -1)).thenReturn(merkleTree);
			when(merkleTree.getRootNodesForInterval(predecessorAddress.getNodeId().add(ONodeId.ONE), localNodeAddress.getNodeId()))
							.thenReturn(Arrays.asList(localRootNode));

			final ODHTNode remoteNodeOne = mock(ODHTNode.class);

			remoteNodeTwo = mock(ODHTNode.class);

			when(nodeLookup.findById(replicaHolderOne)).thenReturn(remoteNodeOne);
			when(nodeLookup.findById(replicaHolderTwo)).thenReturn(remoteNodeTwo);

			when(remoteNodeOne.findMerkleTreeNode(null, localRootNode)).thenReturn(remoteRootNode);
			when(remoteNodeTwo.findMerkleTreeNode(null, localRootNode)).thenReturn(remoteRootNode);
		}
	}

	private class RootNodesAreDifferentRemoteIsLeafButLocalIsNot extends CreateTwoDifferentNodesWithOnlyOneInside {
		private RootNodesAreDifferentRemoteIsLeafButLocalIsNot() {
			final Random random = new Random();

			final byte[] localRootHash = new byte[256];
			random.nextBytes(localRootHash);

			final byte[] remoteRootHash = Arrays.copyOf(localRootHash, 256);
			remoteRootHash[0] = (byte) (remoteRootHash[0] + 1);

			final byte[] childHash = new byte[256];
			random.nextBytes(childHash);

			final byte[] firstLocalDiffChildHash = new byte[256];
			random.nextBytes(firstLocalDiffChildHash);

			final byte[] secondLocalDiffChildHash = new byte[256];
			random.nextBytes(secondLocalDiffChildHash);

			final ORecordMetadata[] recordMetadata = new ORecordMetadata[64];

			final byte[][] localChildHash = new byte[64][];
			for (int i = 0; i < 64; i++) {
				if (i == 2)
					localChildHash[i] = firstLocalDiffChildHash;
				else if (i == 63)
					localChildHash[i] = secondLocalDiffChildHash;
				else
					localChildHash[i] = childHash;
			}

			final ONodeAddress localNodeAddress = localNode.getNodeAddress();

			localRootNode = new ODetachedMerkleTreeNode(localRootHash, predecessorAddress.getNodeId().add(ONodeId.ONE),
							localNodeAddress.getNodeId(), null, localChildHash, new int[0]);

			remoteRootNode = new ODetachedMerkleTreeNode(remoteRootHash, predecessorAddress.getNodeId().add(ONodeId.ONE),
							localNodeAddress.getNodeId(), recordMetadata, null, new int[0]);

			when(localNode.getLocalMerkleTree(null, -1)).thenReturn(merkleTree);
			when(merkleTree.getRootNodesForInterval(predecessorAddress.getNodeId().add(ONodeId.ONE), localNodeAddress.getNodeId()))
							.thenReturn(Arrays.asList(localRootNode));

			final ODHTNode remoteNodeOne = mock(ODHTNode.class);

			remoteNodeTwo = mock(ODHTNode.class);

			when(nodeLookup.findById(replicaHolderOne)).thenReturn(remoteNodeOne);
			when(nodeLookup.findById(replicaHolderTwo)).thenReturn(remoteNodeTwo);

			when(remoteNodeOne.findMerkleTreeNode(null, localRootNode)).thenReturn(remoteRootNode);
			when(remoteNodeTwo.findMerkleTreeNode(null, localRootNode)).thenReturn(remoteRootNode);
		}
	}

	private class PredecessorIsNullSecondTime extends CreateTwoDifferentNodesWithOnlyOneInside {
		private PredecessorIsNullSecondTime() {
			when(localNode.getPredecessor()).thenAnswer(new Answer<Object>() {
				private int count = 0;

				@Override
				public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
					count++;
					if (count < 2)
						return predecessorAddress;

					return null;
				}
			});
		}
	}

	private class OneReplicaHolderThrowsExceptionDuringProcessing extends CreateTwoDifferentNodesWithOnlyOneInside {
		private OneReplicaHolderThrowsExceptionDuringProcessing() {
			when(remoteNodeTwo.findMerkleTreeNode(null, firstLocalChildNode)).thenThrow(new RuntimeException("Fake exception"));
		}
	}

	private class OneReplicaHolderIsOffline extends CreateTwoDifferentNodesWithOnlyOneInside {
		private OneReplicaHolderIsOffline() {
			when(nodeLookup.findById(replicaHolderTwo)).thenReturn(null);
		}
	}

	private class CreateTwoDifferentNodesWithOnlyOneInside {
		public ODetachedMerkleTreeNode localRootNode;
		public ODetachedMerkleTreeNode remoteRootNode;

		public ONodeAddress replicaHolderOne;
		public ONodeAddress replicaHolderTwo;

		public ODetachedMerkleTreeNode firstLocalChildNode;
		public ODetachedMerkleTreeNode firstRemoteChildNode;

		public ONodeAddress predecessorAddress;

		public ODHTNode remoteNodeTwo;
		public ODetachedMerkleTreeNode secondLocalChildNode;
		public ODetachedMerkleTreeNode secondRemoteChildNode;
		public ODHTNode remoteNodeOne;

		private CreateTwoDifferentNodesWithOnlyOneInside() {
			final ONodeAddress localNodeAddress = localNodeAddress();

			predecessorAddress = predecessorAddress();

			when(localNode.getNodeAddress()).thenReturn(localNodeAddress);
			when(localNode.getPredecessor()).thenReturn(predecessorAddress);
			when(localNode.state()).thenReturn(ODHTNode.NodeState.PRODUCTION);

			replicaHolderOne = new ONodeAddressStub(localNodeAddress.getNodeId().add(ONodeId.valueOf(1000)));
			replicaHolderTwo = new ONodeAddressStub(replicaHolderOne.getNodeId().add(ONodeId.valueOf(1000)));

			final ONodeAddress replicaHolderThree = new ONodeAddressStub(replicaHolderTwo.getNodeId().add(ONodeId.valueOf(1000)));

			when(localNode.getSuccessors()).thenReturn(new ONodeAddress[]{replicaHolderOne, replicaHolderTwo, replicaHolderThree});

			when(
							replicaDistributionStrategy.chooseReplicas(new ONodeAddress[]{replicaHolderOne, replicaHolderTwo, replicaHolderThree},
											2, 1)).thenReturn(
							new Set[]{new HashSet(Arrays.asList(replicaHolderOne)), new HashSet(Arrays.asList(replicaHolderTwo))});

			final Random random = new Random();

			final byte[] localRootHash = new byte[256];
			random.nextBytes(localRootHash);

			final byte[] remoteRootHash = Arrays.copyOf(localRootHash, 256);
			remoteRootHash[0] = (byte) (remoteRootHash[0] + 1);

			final byte[] childHash = new byte[256];
			random.nextBytes(childHash);

			final byte[] firstLocalDiffChildHash = new byte[256];
			random.nextBytes(firstLocalDiffChildHash);

			final byte[] firstRemoteDiffChildHash = Arrays.copyOf(firstLocalDiffChildHash, 256);
			firstRemoteDiffChildHash[0] = (byte) (firstLocalDiffChildHash[0] + 1);

			final byte[] secondLocalDiffChildHash = new byte[256];
			random.nextBytes(secondLocalDiffChildHash);

			final byte[] secondRemoteDiffChildHash = Arrays.copyOf(secondLocalDiffChildHash, 256);
			secondLocalDiffChildHash[0] = (byte) (secondRemoteDiffChildHash[0] + 1);

			final ORecordMetadata[] recordMetadata = new ORecordMetadata[64];

			final byte[][] localChildHash = new byte[64][];
			for (int i = 0; i < 64; i++) {
				if (i == 2)
					localChildHash[i] = firstLocalDiffChildHash;
				else if (i == 63)
					localChildHash[i] = secondLocalDiffChildHash;
				else
					localChildHash[i] = childHash;
			}

			final byte[][] remoteChildHash = new byte[64][];
			for (int i = 0; i < 64; i++) {
				if (i == 2)
					localChildHash[i] = firstRemoteDiffChildHash;
				else if (i == 63)
					localChildHash[i] = secondRemoteDiffChildHash;
				else
					localChildHash[i] = childHash;
			}

			localRootNode = new ODetachedMerkleTreeNode(localRootHash,
							localRootNodeStartId(),
							localRootNodeEndId(), null, localChildHash, new int[0]);

			remoteRootNode = new ODetachedMerkleTreeNode(remoteRootHash,
							remoteRootNodeStartId(),
							remoteRootNodeEndId(), null, remoteChildHash, new int[0]);

			final ODetachedMerkleTreeNode localChildNode = new ODetachedMerkleTreeNode(childHash,
							localChildNodeStartId(),
							localChildNodeEndId(), recordMetadata, null, new int[0]);

			final ODetachedMerkleTreeNode remoteChildNode = new ODetachedMerkleTreeNode(childHash,
							remoteChildNodeStartId(),
							remoteChildNodeEndId(), recordMetadata, null, new int[0]);

			firstLocalChildNode = new ODetachedMerkleTreeNode(firstLocalDiffChildHash,
							firstLocalChildNodeStartId(),
							firstLocalChildNodeEndId(), recordMetadata, null, new int[0]);

			firstRemoteChildNode = new ODetachedMerkleTreeNode(firstRemoteDiffChildHash,
							firstRemoteChildNodeStartId(),
							firstRemoteChildNodeEndId(), recordMetadata, null, new int[0]);

			secondLocalChildNode = new ODetachedMerkleTreeNode(secondLocalDiffChildHash,
							secondLocalChildNodeStartId(),
							secondLocalChildNodeEndId(), recordMetadata, null, new int[0]);

			secondRemoteChildNode = new ODetachedMerkleTreeNode(secondRemoteDiffChildHash,
							secondRemoteChildNodeStartId(),
							secondRemoteChildNodeEndId(), recordMetadata, null, new int[0]);

			when(localNode.getLocalMerkleTree(null, -1)).thenReturn(merkleTree);
			when(merkleTree.getRootNodesForInterval(localChildNodeStartId(), localRootNodeEndId()))
							.thenReturn(Arrays.asList(localRootNode));

			remoteNodeOne = mock(ODHTNode.class);

			remoteNodeTwo = mock(ODHTNode.class);

			when(nodeLookup.findById(replicaHolderOne)).thenReturn(remoteNodeOne);
			when(nodeLookup.findById(replicaHolderTwo)).thenReturn(remoteNodeTwo);

			when(remoteNodeOne.findMerkleTreeNode(null, localRootNode)).thenReturn(remoteRootNode);
			when(remoteNodeTwo.findMerkleTreeNode(null, localRootNode)).thenReturn(remoteRootNode);

			for (int i = 0; i < 64; i++) {
				if (i == 2) {
					when(merkleTree.getChildNode(localRootNode, i)).thenReturn(firstLocalChildNode);
					when(remoteNodeOne.findMerkleTreeNode(null, firstLocalChildNode)).thenReturn(firstRemoteChildNode);
					when(remoteNodeTwo.findMerkleTreeNode(null, firstLocalChildNode)).thenReturn(firstRemoteChildNode);
				} else if (i == 63) {
					when(merkleTree.getChildNode(localRootNode, i)).thenReturn(secondLocalChildNode);
					when(remoteNodeOne.findMerkleTreeNode(null, secondLocalChildNode)).thenReturn(secondRemoteChildNode);
					when(remoteNodeTwo.findMerkleTreeNode(null, secondLocalChildNode)).thenReturn(secondRemoteChildNode);
				} else {
					when(merkleTree.getChildNode(localRootNode, i)).thenReturn(localChildNode);
					when(remoteNodeOne.findMerkleTreeNode(null, localChildNode)).thenReturn(remoteChildNode);
					when(remoteNodeTwo.findMerkleTreeNode(null, localChildNode)).thenReturn(remoteChildNode);
				}
			}
		}

		protected ONodeId firstRemoteChildNodeStartId() {
			return predecessorAddress.getNodeId().add(ONodeId.ONE);
		}

		protected ONodeId secondRemoteChildNodeEndId() {
			ONodeAddress localNodeAddress = localNodeAddress();
			return localNodeAddress.getNodeId().add(ONodeId.TWO);
		}

		protected ONodeId secondRemoteChildNodeStartId() {
			ONodeAddress localNodeAddress  = localNodeAddress();
			return localNodeAddress.getNodeId().add(ONodeId.ONE);
		}

		protected ONodeId secondLocalChildNodeEndId() {
			ONodeAddress localNodeAddress = localNodeAddress();
			return localNodeAddress.getNodeId().add(ONodeId.TWO);
		}

		protected ONodeId secondLocalChildNodeStartId() {
			ONodeAddress localNodeAddress = localNodeAddress();
			return localNodeAddress.getNodeId().add(ONodeId.ONE);
		}

		protected ONodeId firstRemoteChildNodeEndId() {
			return predecessorAddress.getNodeId().add(ONodeId.ONE);
		}

		protected ONodeId firstLocalChildNodeEndId() {
			ONodeAddress localNodeAddress = localNodeAddress();
			return localNodeAddress.getNodeId().subtract(ONodeId.TWO);
		}

		protected ONodeId firstLocalChildNodeStartId() {
			return predecessorAddress.getNodeId().add(ONodeId.ONE);
		}

		protected ONodeId remoteChildNodeEndId() {
			ONodeAddress localNodeAddress = localNodeAddress();
			return localNodeAddress.getNodeId().subtract(ONodeId.TWO);
		}

		protected ONodeId remoteChildNodeStartId() {
			return predecessorAddress.getNodeId().add(ONodeId.ONE);
		}

		protected ONodeId localChildNodeEndId() {
			ONodeAddress localNodeAddress= localNodeAddress();
			return localNodeAddress.getNodeId().subtract(ONodeId.TWO);
		}

		protected ONodeId localChildNodeStartId() {
			return predecessorAddress.getNodeId().add(ONodeId.ONE);
		}

		protected ONodeId remoteRootNodeEndId() {
			return localNodeAddress().getNodeId();
		}

		protected ONodeId localRootNodeEndId() {
			return localNodeAddress().getNodeId();
		}

		protected ONodeId remoteRootNodeStartId() {
			return predecessorAddress.getNodeId().add(ONodeId.ONE);
		}

		protected ONodeId localRootNodeStartId() {
			return predecessorAddress.getNodeId().add(ONodeId.ONE);
		}

		protected ONodeAddressStub predecessorAddress() {
			return new ONodeAddressStub(localNodeAddress().getNodeId().subtract(ONodeId.valueOf(10000)));
		}

		protected ONodeAddressStub localNodeAddress() {
			return new ONodeAddressStub(ONodeId.valueOf(20000));
		}
	}
}
