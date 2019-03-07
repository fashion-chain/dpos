package org.brewchain.dposblk;

import org.brewchain.bcapi.backend.ODBDao;

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
