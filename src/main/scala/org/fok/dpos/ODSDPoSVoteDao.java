package org.fok.dpos;

import org.fok.core.dbapi.ODBDao;

import onight.tfw.ojpa.api.ServiceSpec;

public class ODSDPoSVoteDao extends ODBDao {

	public ODSDPoSVoteDao(ServiceSpec serviceSpec) {
		super(serviceSpec);
	}

	@Override
	public String getDomainName() {
		return "dpos.vote";
	}

	
}
