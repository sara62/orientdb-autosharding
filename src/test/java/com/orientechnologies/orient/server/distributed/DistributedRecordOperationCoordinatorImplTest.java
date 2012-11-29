package com.orientechnologies.orient.server.distributed;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.orientechnologies.orient.core.id.OClusterPositionNodeId;
import com.orientechnologies.orient.core.id.ONodeId;
import com.orientechnologies.orient.core.id.ORecordId;
import junit.framework.Assert;
import org.mockito.ArgumentCaptor;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.id.OClusterPosition;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.server.distributed.operations.ODistributedRecordOperation;
import com.orientechnologies.orient.server.distributed.operations.ODistributedRecordOperationCoordinatorImpl;

/**
 * @author Andrey Lomakin
 * @since 27.11.12
 */
@Test
public class DistributedRecordOperationCoordinatorImplTest {
  public void testExecuteRecordOperationRecordIdIsAbsent() {
		final ODHTNodeLookup nodeLookup = mock(ODHTNodeLookup.class);
		final ODistributedRecordOperationStub recordOperationStub = new ODistributedRecordOperationStub();

		ODistributedRecordOperationCoordinatorImpl recordOperationCoordinator =
						new ODistributedRecordOperationCoordinatorImpl(nodeLookup);

		final ODHTNode node = mock(ODHTNode.class);
		when(node.state()).thenReturn(ODHTNode.NodeState.PRODUCTION);

		when(node.getNodeAddress()).thenAnswer(new Answer<ONodeAddress>() {
			@Override
			public ONodeAddress answer(InvocationOnMock invocationOnMock) throws Throwable {
				return new ONodeAddressStub(((OClusterPositionNodeId)recordOperationStub.recordId.getClusterPosition()).getNodeId());
			}
		});


		when(node.findSuccessor((ONodeId)any())).thenAnswer(new Answer<ONodeAddress>() {
			@Override
			public ONodeAddress answer(InvocationOnMock invocationOnMock) throws Throwable {
				return new ONodeAddressStub(((OClusterPositionNodeId)recordOperationStub.recordId.getClusterPosition()).getNodeId());
			}
		});

		int result = recordOperationCoordinator.executeRecordOperation(node, recordOperationStub);

		Assert.assertEquals(11, result);
		Assert.assertSame(node, recordOperationStub.getExecutedNode());

		ArgumentCaptor<ONodeId> argument = ArgumentCaptor.forClass(ONodeId.class);
		verify(node).findSuccessor(argument.capture());

		Assert.assertEquals(((OClusterPositionNodeId)recordOperationStub.recordId.getClusterPosition()).getNodeId(), argument.getValue());
	}

	public void testExecuteRecordOperationLocalNode() {
		final ODHTNodeLookup nodeLookup = mock(ODHTNodeLookup.class);

		final OClusterPositionNodeId clusterPosition = new OClusterPositionNodeId(ONodeId.generateUniqueId());
		final ONodeAddress nodeAddress = new ONodeAddressStub(clusterPosition.getNodeId());

		final ODHTNode node = mock(ODHTNode.class);
		when(node.state()).thenReturn(ODHTNode.NodeState.PRODUCTION);
		when(node.getNodeAddress()).thenReturn(nodeAddress);

		when(node.findSuccessor(clusterPosition.getNodeId())).thenReturn(nodeAddress);

		final ODistributedRecordOperationStub recordOperationStub = new ODistributedRecordOperationStub();
		recordOperationStub.setRecordId(new ORecordId(1, clusterPosition));

		ODistributedRecordOperationCoordinatorImpl recordOperationCoordinator =
						new ODistributedRecordOperationCoordinatorImpl(nodeLookup);

		int result = recordOperationCoordinator.executeRecordOperation(node, recordOperationStub);

		Assert.assertEquals(11, result);
		Assert.assertSame(node, recordOperationStub.getExecutedNode());
	}

	public void testExecuteRecordOperationRemoteNode() {
		final ODHTNodeLookup nodeLookup = mock(ODHTNodeLookup.class);

		final OClusterPositionNodeId clusterPosition = new OClusterPositionNodeId(ONodeId.generateUniqueId());

		final ONodeAddress nodeAddress = new ONodeAddressStub(clusterPosition.getNodeId());
		final ONodeAddress remoteNodeAddress = new ONodeAddressStub(ONodeId.generateUniqueId());

		final ODHTNode node = mock(ODHTNode.class);
		when(node.state()).thenReturn(ODHTNode.NodeState.PRODUCTION);
		when(node.getNodeAddress()).thenReturn(nodeAddress);

		when(node.findSuccessor(clusterPosition.getNodeId())).thenReturn(remoteNodeAddress);

		final ODistributedRecordOperationStub recordOperationStub = new ODistributedRecordOperationStub();
		recordOperationStub.setRecordId(new ORecordId(1, clusterPosition));

		ODistributedRecordOperationCoordinatorImpl recordOperationCoordinator =
						new ODistributedRecordOperationCoordinatorImpl(nodeLookup);

		final ODHTNode remoteNode = mock(ODHTNode.class);

		when(nodeLookup.findById(remoteNodeAddress)).thenReturn(remoteNode);

		int result = recordOperationCoordinator.executeRecordOperation(node, recordOperationStub);

		Assert.assertEquals(11, result);
		Assert.assertSame(remoteNode, recordOperationStub.getExecutedNode());
	}

	public void testExecuteRecordOperationRemoteNodeOfflineNull() {
		final ODHTNodeLookup nodeLookup = mock(ODHTNodeLookup.class);

		final OClusterPositionNodeId clusterPosition = new OClusterPositionNodeId(ONodeId.generateUniqueId());

		final ONodeAddress nodeAddress = new ONodeAddressStub(clusterPosition.getNodeId());
		final ONodeAddress remoteNodeAddress = new ONodeAddressStub(ONodeId.generateUniqueId());

		final ODHTNode node = mock(ODHTNode.class);
		when(node.state()).thenReturn(ODHTNode.NodeState.PRODUCTION);
		when(node.getNodeAddress()).thenReturn(nodeAddress);

		when(node.findSuccessor(clusterPosition.getNodeId())).thenReturn(remoteNodeAddress);

		final ODistributedRecordOperationStub recordOperationStub = new ODistributedRecordOperationStub();
		recordOperationStub.setRecordId(new ORecordId(1, clusterPosition));

		ODistributedRecordOperationCoordinatorImpl recordOperationCoordinator =
						new ODistributedRecordOperationCoordinatorImpl(nodeLookup);

	  when(nodeLookup.findById(remoteNodeAddress)).thenReturn(null);

		boolean exceptionIsThrown = false;
		try{
			recordOperationCoordinator.executeRecordOperation(node, recordOperationStub);
			Assert.fail();
		} catch (ONodeOfflineException e) {
			exceptionIsThrown = true;
		}

		Assert.assertTrue(exceptionIsThrown);
	}

	public void testExecuteRecordOperationRemoteNodeOfflineOfflineException() {
		final ODHTNodeLookup nodeLookup = mock(ODHTNodeLookup.class);

		final OClusterPositionNodeId clusterPosition = new OClusterPositionNodeId(ONodeId.generateUniqueId());

		final ONodeAddress nodeAddress = new ONodeAddressStub(clusterPosition.getNodeId());
		final ONodeAddress remoteNodeAddress = new ONodeAddressStub(ONodeId.generateUniqueId());

		final ODHTNode node = mock(ODHTNode.class);
		when(node.state()).thenReturn(ODHTNode.NodeState.PRODUCTION);
		when(node.getNodeAddress()).thenReturn(nodeAddress);

		when(node.findSuccessor(clusterPosition.getNodeId())).thenReturn(remoteNodeAddress);

		final ODHTNode remoteNode = mock(ODHTNode.class);
		final ODistributedRecordOperation recordOperation = mock(ODistributedRecordOperation.class);
		when(recordOperation.getClusterPosition()).thenReturn(clusterPosition);

		when(recordOperation.execute(remoteNode)).
						thenThrow(new ONodeOfflineException("", null, remoteNodeAddress.getNodeId()));

		ODistributedRecordOperationCoordinatorImpl recordOperationCoordinator =
						new ODistributedRecordOperationCoordinatorImpl(nodeLookup);


		when(nodeLookup.findById(remoteNodeAddress)).thenReturn(remoteNode);

		boolean exceptionIsThrown = false;
		try{
			recordOperationCoordinator.executeRecordOperation(node, recordOperation);
			Assert.fail();
		} catch (ONodeOfflineException e) {
			exceptionIsThrown = true;
		}

		Assert.assertTrue(exceptionIsThrown);
	}


	private final class ODistributedRecordOperationStub implements ODistributedRecordOperation<Integer> {
    private ORID recordId;
		private ODHTNode executedNode;

    @Override
    public Integer execute(ODHTNode node) {
			executedNode = node;

      return 11;
    }

    @Override
    public OClusterPosition getClusterPosition() {
      if (recordId != null)
        return recordId.getClusterPosition();

      return null;
    }

    @Override
    public int getClusterId() {
      return 1;
    }

    @Override
    public void setRecordId(ORID recordId) {
      this.recordId = recordId;
    }

		public ODHTNode getExecutedNode() {
			return executedNode;
		}
	}

	private final class ONodeAddressStub extends ONodeAddress {
		private ONodeAddressStub(ONodeId nodeId) {
			super(nodeId);
		}
	}
}
