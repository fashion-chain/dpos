package org.fok.dpos;

import org.fok.core.dbapi.ODBDao;

import onight.tfw.ojpa.api.ServiceSpec;

public class ODSDPoSDao extends ODBDao {

	public ODSDPoSDao(ServiceSpec serviceSpec) {
		super(serviceSpec);
	}

	@Override
	public String getDomainName() {
		return "dpos.prop";
	}

	
}
