package org.fok.dpos;

import org.fok.core.dbapi.ODBDao;

import onight.tfw.ojpa.api.ServiceSpec;

public class ODSBlkDao extends ODBDao {

	public ODSBlkDao(ServiceSpec serviceSpec) {
		super(serviceSpec);
	}

	@Override
	public String getDomainName() {
		return "dpos.blk";
	}

	
}
