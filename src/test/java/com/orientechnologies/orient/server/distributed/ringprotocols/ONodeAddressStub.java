package com.orientechnologies.orient.server.distributed.ringprotocols;

import com.orientechnologies.orient.core.id.ONodeId;
import com.orientechnologies.orient.server.distributed.ONodeAddress;

/**
* @author Andrey Lomakin
* @since 30.11.12
*/
final class ONodeAddressStub extends ONodeAddress {
	ONodeAddressStub(ONodeId nodeId) {
		super(nodeId);
	}
}