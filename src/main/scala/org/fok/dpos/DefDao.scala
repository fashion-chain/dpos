package org.fok.dpos

import scala.beans.BeanProperty

import com.google.protobuf.Message
import onight.oapi.scala.commons.SessionModules
import onight.oapi.scala.commons.PBUtils
import onight.oapi.scala.traits.OLog
import onight.osgi.annotation.NActorProvider
import onight.tfw.ntrans.api.ActorService
import onight.tfw.ojpa.api.IJPAClient
import onight.tfw.ojpa.api.annotations.StoreDAO
import org.fok.core.dbapi.ODBSupport
import org.apache.felix.ipojo.annotations.Provides
import onight.tfw.ojpa.api.DomainDaoSupport
import onight.tfw.ntrans.api.annotation.ActorRequire
import org.fok.p22p.core.PZPCtrl
import org.fok.core.FokAccount
import org.fok.core.FokBlock
import org.fok.core.FokBlockChain
import org.fok.core.FokTransaction
import org.fok.core.cryptoapi.ICryptoHandler
import org.fok.dpos.model.Dposblock.PModule;

abstract class PSMDPoSNet[T <: Message] extends SessionModules[T] with PBUtils with OLog {
  override def getModule: String = PModule.DOB.name()
}

@NActorProvider
@Provides(specifications = Array(classOf[ActorService], classOf[IJPAClient]))
class Daos extends PSMDPoSNet[Message] with ActorService {

  @StoreDAO(target = "fok_db", daoClass = classOf[ODSDPoSDao])
  @BeanProperty
  var dposdb: ODBSupport = null

  
  @StoreDAO(target = "fok_db", daoClass = classOf[ODSDPoSVoteDao])
  @BeanProperty
  var dposvotedb: ODBSupport = null
  
  //  @StoreDAO(target = "bc_bdb", daoClass = classOf[ODSBlkDao])
  //  @BeanProperty
  //  var blkdb: ODBSupport = null

  def setDposdb(daodb: DomainDaoSupport) {
    if (daodb != null && daodb.isInstanceOf[ODBSupport]) {
      dposdb = daodb.asInstanceOf[ODBSupport];
      Daos.dpospropdb = dposdb;
    } else {
      log.warn("cannot set dposdb ODBSupport from:" + daodb);
    }
  }
  
   def setDposvotedb(daodb: DomainDaoSupport) {
    if (daodb != null && daodb.isInstanceOf[ODBSupport]) {
      dposvotedb = daodb.asInstanceOf[ODBSupport];
      Daos.dposvotedb = dposvotedb;
    } else {
      log.warn("cannot set dposdb ODBSupport from:" + daodb);
    }
  }

  //  def setBlkdb(daodb: DomainDaoSupport) {
  //    if (daodb != null && daodb.isInstanceOf[ODBSupport]) {
  //      blkdb = daodb.asInstanceOf[ODBSupport];
  //      Daos.blkdb = blkdb;
  //    } else {
  //      log.warn("cannot set blkdb ODBSupport from:" + daodb);
  //    }
  //  }

  @ActorRequire(scope = "global", name = "pzpctrl")
  var pzp: PZPCtrl = null;

  @ActorRequire(name = "fok_block_chain_core", scope = "global")
  var bcHelper: FokBlockChain = null;

  @ActorRequire(name = "fok_block_core", scope = "global")
  var blkHelper: FokBlock = null;
  
  @ActorRequire(name = "fok_transaction_core", scope = "global")
  var txHelper: FokTransaction = null;

  @ActorRequire(name = "bc_crypto", scope = "global") //  @BeanProperty
  var enc: ICryptoHandler = null;
    
  def setPzp(_pzp: PZPCtrl) = {
    pzp = _pzp;
    Daos.pzp = pzp;
  }
  def getPzp(): PZPCtrl = {
    pzp
  }
  def setBcHelper(_bcHelper: FokBlockChain) = {
    bcHelper = _bcHelper;
    Daos.actdb = bcHelper;
  }
  def getBcHelper: FokBlockChain = {
    bcHelper
  }

  def setBlkHelper(_blkHelper: FokBlock) = {
    blkHelper = _blkHelper;
    Daos.blkHelper = _blkHelper;
  }
  def getBlkHelper: FokBlock = {
    blkHelper
  }
  
  def setTxHelper(_txHelper: FokTransaction) = {
    txHelper = _txHelper;
    Daos.txHelper = _txHelper;
  }
  def getTxHelper: FokTransaction = {
    txHelper
  }
  
  def setEnc(_enc: ICryptoHandler) = {
    enc = _enc;
    Daos.enc = _enc;
  }
  def getEnc(): ICryptoHandler = {
    enc;
  }
  
}

object Daos extends OLog {
  var dpospropdb: ODBSupport = null
  var dposvotedb:ODBSupport = null
  //  var blkdb: ODBSupport = null
  var pzp: PZPCtrl = null;
  var actdb: FokBlockChain = null; 
  var blkHelper: FokBlock = null;
  var txHelper: FokTransaction = null;
  var enc: ICryptoHandler = null;
  def isDbReady(): Boolean = {
    dpospropdb != null && dpospropdb.getDaosupport.isInstanceOf[ODBSupport] &&
    dposvotedb != null && dposvotedb.getDaosupport.isInstanceOf[ODBSupport] &&
      blkHelper != null &&
      txHelper != null &&
      pzp != null && actdb != null;
  }
}



