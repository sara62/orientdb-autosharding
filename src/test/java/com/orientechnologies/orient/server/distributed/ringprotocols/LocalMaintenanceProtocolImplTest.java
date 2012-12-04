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

		localMaintenanceProtocol.synchronizeReplicasBetweenHolders(localNode, 2, 1);

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

		when(localNode.getLocalMerkleTree()).thenReturn(merkleTree);
		when(merkleTree.getRootNodesForInterval(predecessorAddress.getNodeId().add(ONodeId.ONE), localNodeAddress.getNodeId()))
						.thenReturn(Arrays.asList(localRootNode));

		final ODHTNode remoteNodeOne = mock(ODHTNode.class);
		final ODHTNode remoteNodeTwo = mock(ODHTNode.class);

		when(nodeLookup.findById(replicaHolderOne)).thenReturn(remoteNodeOne);
		when(nodeLookup.findById(replicaHolderTwo)).thenReturn(remoteNodeTwo);

		when(remoteNodeOne.findMerkleTreeNode(localRootNode)).thenReturn(remoteRootNode);
		when(remoteNodeTwo.findMerkleTreeNode(localRootNode)).thenReturn(remoteRootNode);

		localMaintenanceProtocol.synchronizeReplicasBetweenHolders(localNode, 2, 1);

		verifyZeroInteractions(nodeComparator);
	}

	public void testVerifyOneChildInsideRootIntervalAndSecondIsNot() {
		final CreateTwoDifferentNodesWithOnlyOneInside processFlow = new CreateTwoDifferentNodesWithOnlyOneInside();
		localMaintenanceProtocol.synchronizeReplicasBetweenHolders(localNode, 2, 1);

		verify(nodeComparator).compareNodes(localNode, processFlow.localRootNode, processFlow.remoteRootNode,
						processFlow.replicaHolderOne);

		verify(nodeComparator).compareNodes(localNode, processFlow.localRootNode, processFlow.remoteRootNode,
						processFlow.replicaHolderTwo);

		verify(nodeComparator).compareNodes(localNode, processFlow.firstLocalChildNode, processFlow.firstRemoteChildNode,
						processFlow.replicaHolderOne);

		verify(nodeComparator).compareNodes(localNode, processFlow.firstLocalChildNode, processFlow.firstRemoteChildNode,
						processFlow.replicaHolderTwo);

		verifyNoMoreInteractions(nodeComparator);
	}

	public void testOneOfTheServerNodesThrowsExceptionDuringProcessing() {
		final OneReplicaHolderThrowsExceptionDuringProcessing processFlow = new OneReplicaHolderThrowsExceptionDuringProcessing();

		localMaintenanceProtocol.synchronizeReplicasBetweenHolders(localNode, 2, 1);

		verify(nodeComparator).compareNodes(localNode, processFlow.localRootNode, processFlow.remoteRootNode,
						processFlow.replicaHolderOne);

		verify(nodeComparator).compareNodes(localNode, processFlow.localRootNode, processFlow.remoteRootNode,
						processFlow.replicaHolderTwo);

		verify(nodeComparator).compareNodes(localNode, processFlow.firstLocalChildNode, processFlow.firstRemoteChildNode,
						processFlow.replicaHolderOne);

		verifyNoMoreInteractions(nodeComparator);
	}

	public void testOneOfTheServerNodesIsOffline() {
		final OneReplicaHolderIsOffline processFlow = new OneReplicaHolderIsOffline();

		localMaintenanceProtocol.synchronizeReplicasBetweenHolders(localNode, 2, 1);

		verify(nodeComparator).compareNodes(localNode, processFlow.localRootNode, processFlow.remoteRootNode,
						processFlow.replicaHolderOne);

		verify(nodeComparator).compareNodes(localNode, processFlow.firstLocalChildNode, processFlow.firstRemoteChildNode,
						processFlow.replicaHolderOne);

		verifyNoMoreInteractions(nodeComparator);
	}

	public void testPredecessorIsNullSecondTime() {
		final PredecessorIsNullSecondTime processFlow = new PredecessorIsNullSecondTime();

		localMaintenanceProtocol.synchronizeReplicasBetweenHolders(localNode, 2, 1);

		verifyZeroInteractions(nodeComparator);
	}

	public void testRemoteTreeNodeIsNull() {
		RemoteTreeNodeIsNull processFlow = new RemoteTreeNodeIsNull();

		localMaintenanceProtocol.synchronizeReplicasBetweenHolders(localNode, 2, 1);

		verify(nodeComparator).compareNodes(localNode, processFlow.localRootNode, processFlow.remoteRootNode,
						processFlow.replicaHolderOne);

		verify(nodeComparator).compareNodes(localNode, processFlow.localRootNode, processFlow.remoteRootNode,
						processFlow.replicaHolderTwo);

		verify(nodeComparator).compareNodes(localNode, processFlow.firstLocalChildNode, processFlow.firstRemoteChildNode,
						processFlow.replicaHolderOne);

		verifyNoMoreInteractions(nodeComparator);
	}

	public void testLocalTreeChildNodeIsNull() {
		LocalTreeNodeIsNull processFlow = new LocalTreeNodeIsNull();

		localMaintenanceProtocol.synchronizeReplicasBetweenHolders(localNode, 2, 1);

		verify(nodeComparator).compareNodes(localNode, processFlow.localRootNode, processFlow.remoteRootNode,
						processFlow.replicaHolderOne);

		verify(nodeComparator).compareNodes(localNode, processFlow.localRootNode, processFlow.remoteRootNode,
						processFlow.replicaHolderTwo);

		verifyNoMoreInteractions(nodeComparator);
	}

	public void testRootNodesAreDifferentRemoteIsLeafButLocalIsNot() {
		RootNodesAreDifferentRemoteIsLeafButLocalIsNot processFlow = new RootNodesAreDifferentRemoteIsLeafButLocalIsNot();

		localMaintenanceProtocol.synchronizeReplicasBetweenHolders(localNode, 2, 1);

		verify(nodeComparator).compareNodes(localNode, processFlow.localRootNode, processFlow.remoteRootNode,
						processFlow.replicaHolderOne);

		verify(nodeComparator).compareNodes(localNode, processFlow.localRootNode, processFlow.remoteRootNode,
						processFlow.replicaHolderTwo);

		verifyNoMoreInteractions(nodeComparator);
	}


	public void testRootNodesAreDifferentLocalIsLeafButRemoteIsNot() {
		RootNodesAreDifferentLocalIsLeafButRemoteIsNot processFlow = new RootNodesAreDifferentLocalIsLeafButRemoteIsNot();

		localMaintenanceProtocol.synchronizeReplicasBetweenHolders(localNode, 2, 1);

		verify(nodeComparator).compareNodes(localNode, processFlow.localRootNode, processFlow.remoteRootNode,
						processFlow.replicaHolderOne);

		verify(nodeComparator).compareNodes(localNode, processFlow.localRootNode, processFlow.remoteRootNode,
						processFlow.replicaHolderTwo);

		verifyNoMoreInteractions(nodeComparator);
	}

	public void testLeafChildIsNotEqualOnThirdLevel() {
		LeafChildIsNotEqualOnThirdLevel processFlow = new LeafChildIsNotEqualOnThirdLevel();

		localMaintenanceProtocol.synchronizeReplicasBetweenHolders(localNode, 2, 1);

		verify(nodeComparator).compareNodes(localNode, processFlow.localRootNode, processFlow.remoteRootNode,
						processFlow.replicaHolderOne);
		verify(nodeComparator).compareNodes(localNode, processFlow.localRootNode, processFlow.remoteRootNode,
						processFlow.replicaHolderTwo);

		verify(nodeComparator).compareNodes(localNode, processFlow.firstLocalChildNode, processFlow.firstRemoteChildNode,
						processFlow.replicaHolderOne);
		verify(nodeComparator).compareNodes(localNode, processFlow.firstLocalChildNode, processFlow.firstRemoteChildNode,
						processFlow.replicaHolderTwo);

		verify(nodeComparator).compareNodes(localNode, processFlow.firstLocalChildNodeSecondLevel,
						processFlow.firstRemoteChildNodeSecondLevel,
						processFlow.replicaHolderOne);
		verify(nodeComparator).compareNodes(localNode, processFlow.firstLocalChildNodeSecondLevel,
						processFlow.firstRemoteChildNodeSecondLevel,
						processFlow.replicaHolderTwo);

		verifyNoMoreInteractions(nodeComparator);
	}

//	public void testPredecessorIsGreaterThanNodeIdSecondIntervalIsEmpty() {
//		PredecessorIsGreaterThanNodeIdSecondIntervalIsEmpty processFlow =
//						new PredecessorIsGreaterThanNodeIdSecondIntervalIsEmpty();
//
//		localMaintenanceProtocol.synchronizeReplicasBetweenHolders(localNode, 2, 1);
//	}

	private class LocalTreeNodeIsNull extends CreateTwoDifferentNodesWithOnlyOneInside {
		private LocalTreeNodeIsNull() {
			when(merkleTree.getChildNode(localRootNode, 0)).thenReturn(null);
		}
	}

	private class RemoteTreeNodeIsNull extends CreateTwoDifferentNodesWithOnlyOneInside {
		private RemoteTreeNodeIsNull() {
			when(remoteNodeTwo.findMerkleTreeNode(firstLocalChildNode)).thenReturn(null);
		}
	}

	private class PredecessorIsGreaterThanNodeIdSecondIntervalIsEmpty extends CreateTwoDifferentNodesWithOnlyOneInside {
		private PredecessorIsGreaterThanNodeIdSecondIntervalIsEmpty() {
			when(merkleTree.getRootNodesForInterval(ONodeId.valueOf(10001), ONodeId.MAX_VALUE)).
							thenReturn(Arrays.asList(localRootNode));
		}

		@Override
		protected ONodeAddressStub getLocalNodeAddress() {
			return new ONodeAddressStub(ONodeId.valueOf(1000));
		}

		@Override
		protected ONodeAddressStub getPredecessorAddress(ONodeAddress localNodeAddress) {
			return new ONodeAddressStub(ONodeId.valueOf(10000));
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


			when(localNode.getLocalMerkleTree()).thenReturn(merkleTree);
			when(merkleTree.getRootNodesForInterval(predecessorAddress.getNodeId().add(ONodeId.ONE), localNodeAddress.getNodeId()))
							.thenReturn(Arrays.asList(localRootNode));

			final ODHTNode remoteNodeOne = mock(ODHTNode.class);

			remoteNodeTwo = mock(ODHTNode.class);

			when(nodeLookup.findById(replicaHolderOne)).thenReturn(remoteNodeOne);
			when(nodeLookup.findById(replicaHolderTwo)).thenReturn(remoteNodeTwo);

			when(remoteNodeOne.findMerkleTreeNode(localRootNode)).thenReturn(remoteRootNode);
			when(remoteNodeTwo.findMerkleTreeNode(localRootNode)).thenReturn(remoteRootNode);

			for (int i = 0; i < 64; i++) {
				if (i == 2) {
					when(merkleTree.getChildNode(localRootNode, i)).thenReturn(firstLocalChildNode);
					when(remoteNodeOne.findMerkleTreeNode(firstLocalChildNode)).thenReturn(firstRemoteChildNode);
					when(remoteNodeTwo.findMerkleTreeNode(firstLocalChildNode)).thenReturn(firstRemoteChildNode);
				} else if (i == 63) {
					when(merkleTree.getChildNode(localRootNode, i)).thenReturn(secondLocalChildNode);
					when(remoteNodeOne.findMerkleTreeNode(secondLocalChildNode)).thenReturn(secondRemoteChildNode);
					when(remoteNodeTwo.findMerkleTreeNode(secondLocalChildNode)).thenReturn(secondRemoteChildNode);
				} else {
					when(merkleTree.getChildNode(localRootNode, i)).thenReturn(localChildNode);
					when(remoteNodeOne.findMerkleTreeNode(localChildNode)).thenReturn(remoteChildNode);
					when(remoteNodeTwo.findMerkleTreeNode(localChildNode)).thenReturn(remoteChildNode);
				}
			}

			for (int i = 0; i < 64; i++) {
				if (i == 0) {
					when(merkleTree.getChildNode(firstLocalChildNode, i)).thenReturn(firstLocalChildNodeSecondLevel);
					when(remoteNodeOne.findMerkleTreeNode(firstLocalChildNodeSecondLevel)).thenReturn(firstRemoteChildNodeSecondLevel);
					when(remoteNodeTwo.findMerkleTreeNode(firstLocalChildNodeSecondLevel)).thenReturn(firstRemoteChildNodeSecondLevel);
				} else {
					when(merkleTree.getChildNode(firstLocalChildNode, i)).thenReturn(localChildNodeSecondLevel);
					when(remoteNodeOne.findMerkleTreeNode(localChildNodeSecondLevel)).thenReturn(remoteChildNodeSecondLevel);
					when(remoteNodeTwo.findMerkleTreeNode(localChildNodeSecondLevel)).thenReturn(remoteChildNodeSecondLevel);
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

			when(localNode.getLocalMerkleTree()).thenReturn(merkleTree);
			when(merkleTree.getRootNodesForInterval(predecessorAddress.getNodeId().add(ONodeId.ONE), localNodeAddress.getNodeId()))
							.thenReturn(Arrays.asList(localRootNode));

			final ODHTNode remoteNodeOne = mock(ODHTNode.class);

			remoteNodeTwo = mock(ODHTNode.class);

			when(nodeLookup.findById(replicaHolderOne)).thenReturn(remoteNodeOne);
			when(nodeLookup.findById(replicaHolderTwo)).thenReturn(remoteNodeTwo);

			when(remoteNodeOne.findMerkleTreeNode(localRootNode)).thenReturn(remoteRootNode);
			when(remoteNodeTwo.findMerkleTreeNode(localRootNode)).thenReturn(remoteRootNode);
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

			when(localNode.getLocalMerkleTree()).thenReturn(merkleTree);
			when(merkleTree.getRootNodesForInterval(predecessorAddress.getNodeId().add(ONodeId.ONE), localNodeAddress.getNodeId()))
							.thenReturn(Arrays.asList(localRootNode));

			final ODHTNode remoteNodeOne = mock(ODHTNode.class);

			remoteNodeTwo = mock(ODHTNode.class);

			when(nodeLookup.findById(replicaHolderOne)).thenReturn(remoteNodeOne);
			when(nodeLookup.findById(replicaHolderTwo)).thenReturn(remoteNodeTwo);

			when(remoteNodeOne.findMerkleTreeNode(localRootNode)).thenReturn(remoteRootNode);
			when(remoteNodeTwo.findMerkleTreeNode(localRootNode)).thenReturn(remoteRootNode);
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
			when(remoteNodeTwo.findMerkleTreeNode(firstLocalChildNode)).thenThrow(new RuntimeException("Fake exception"));
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

		private CreateTwoDifferentNodesWithOnlyOneInside() {
			final ONodeAddress localNodeAddress = getLocalNodeAddress();

			predecessorAddress = getPredecessorAddress(localNodeAddress);

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

			localRootNode = new ODetachedMerkleTreeNode(localRootHash, predecessorAddress.getNodeId().add(ONodeId.ONE),
							localNodeAddress.getNodeId(), null, localChildHash, new int[0]);

			remoteRootNode = new ODetachedMerkleTreeNode(remoteRootHash, predecessorAddress.getNodeId().add(ONodeId.ONE),
							localNodeAddress.getNodeId(), null, remoteChildHash, new int[0]);

			final ODetachedMerkleTreeNode localChildNode = new ODetachedMerkleTreeNode(childHash, predecessorAddress.getNodeId().add(
							ONodeId.ONE), localNodeAddress.getNodeId().subtract(ONodeId.TWO), recordMetadata, null, new int[0]);

			final ODetachedMerkleTreeNode remoteChildNode = new ODetachedMerkleTreeNode(childHash, predecessorAddress.getNodeId().add(
							ONodeId.ONE), localNodeAddress.getNodeId().subtract(ONodeId.TWO), recordMetadata, null, new int[0]);

			firstLocalChildNode = new ODetachedMerkleTreeNode(firstLocalDiffChildHash, predecessorAddress.getNodeId().add(ONodeId.ONE),
							localNodeAddress.getNodeId().subtract(ONodeId.TWO), recordMetadata, null, new int[0]);

			firstRemoteChildNode = new ODetachedMerkleTreeNode(firstRemoteDiffChildHash, predecessorAddress.getNodeId().add(ONodeId.ONE),
							localNodeAddress.getNodeId().subtract(ONodeId.TWO), recordMetadata, null, new int[0]);

			final ODetachedMerkleTreeNode secondLocalChildNode = new ODetachedMerkleTreeNode(secondLocalDiffChildHash, localNodeAddress
							.getNodeId().add(ONodeId.ONE), localNodeAddress.getNodeId().add(ONodeId.TWO), recordMetadata, null, new int[0]);

			final ODetachedMerkleTreeNode secondRemoteChildNode = new ODetachedMerkleTreeNode(secondRemoteDiffChildHash, localNodeAddress
							.getNodeId().add(ONodeId.ONE), localNodeAddress.getNodeId().add(ONodeId.TWO), recordMetadata, null, new int[0]);

			when(localNode.getLocalMerkleTree()).thenReturn(merkleTree);
			when(merkleTree.getRootNodesForInterval(predecessorAddress.getNodeId().add(ONodeId.ONE), localNodeAddress.getNodeId()))
							.thenReturn(Arrays.asList(localRootNode));

			final ODHTNode remoteNodeOne = mock(ODHTNode.class);

			remoteNodeTwo = mock(ODHTNode.class);

			when(nodeLookup.findById(replicaHolderOne)).thenReturn(remoteNodeOne);
			when(nodeLookup.findById(replicaHolderTwo)).thenReturn(remoteNodeTwo);

			when(remoteNodeOne.findMerkleTreeNode(localRootNode)).thenReturn(remoteRootNode);
			when(remoteNodeTwo.findMerkleTreeNode(localRootNode)).thenReturn(remoteRootNode);

			for (int i = 0; i < 64; i++) {
				if (i == 2) {
					when(merkleTree.getChildNode(localRootNode, i)).thenReturn(firstLocalChildNode);
					when(remoteNodeOne.findMerkleTreeNode(firstLocalChildNode)).thenReturn(firstRemoteChildNode);
					when(remoteNodeTwo.findMerkleTreeNode(firstLocalChildNode)).thenReturn(firstRemoteChildNode);
				} else if (i == 63) {
					when(merkleTree.getChildNode(localRootNode, i)).thenReturn(secondLocalChildNode);
					when(remoteNodeOne.findMerkleTreeNode(secondLocalChildNode)).thenReturn(secondRemoteChildNode);
					when(remoteNodeTwo.findMerkleTreeNode(secondLocalChildNode)).thenReturn(secondRemoteChildNode);
				} else {
					when(merkleTree.getChildNode(localRootNode, i)).thenReturn(localChildNode);
					when(remoteNodeOne.findMerkleTreeNode(localChildNode)).thenReturn(remoteChildNode);
					when(remoteNodeTwo.findMerkleTreeNode(localChildNode)).thenReturn(remoteChildNode);
				}
			}
		}

		protected ONodeAddressStub getPredecessorAddress(ONodeAddress localNodeAddress) {
			return new ONodeAddressStub(localNodeAddress.getNodeId().subtract(ONodeId.valueOf(10000)));
		}

		protected ONodeAddressStub getLocalNodeAddress() {
			return new ONodeAddressStub(ONodeId.valueOf(20000));
		}
	}
}