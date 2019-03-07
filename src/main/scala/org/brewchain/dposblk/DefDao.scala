package org.brewchain.dposblk

import scala.beans.BeanProperty

import com.google.protobuf.Message
import onight.oapi.scala.commons.SessionModules
import onight.oapi.scala.commons.PBUtils
import onight.oapi.scala.traits.OLog
import org.brewchain.dposblk.pbgens.Dposblock.PModule
import onight.osgi.annotation.NActorProvider
import onight.tfw.ntrans.api.ActorService
import onight.tfw.ojpa.api.IJPAClient
import onight.tfw.ojpa.api.annotations.StoreDAO
import org.brewchain.bcapi.backend.ODBSupport
import org.apache.felix.ipojo.annotations.Provides
import onight.tfw.ojpa.api.DomainDaoSupport
import onight.tfw.ntrans.api.annotation.ActorRequire
import org.fc.brewchain.p22p.core.PZPCtrl
import org.brewchain.account.core.BlockChainHelper
import org.brewchain.account.core.BlockHelper
import org.brewchain.account.core.TransactionHelper
import org.fc.brewchain.bcapi.EncAPI

abstract class PSMDPoSNet[T <: Message] extends SessionModules[T] with PBUtils with OLog {
  override def getModule: String = PModule.DOB.name()
}

@NActorProvider
@Provides(specifications = Array(classOf[ActorService], classOf[IJPAClient]))
class Daos extends PSMDPoSNet[Message] with ActorService {

  @StoreDAO(target = "bc_bdb", daoClass = classOf[ODSDPoSDao])
  @BeanProperty
  var dposdb: ODBSupport = null

  
  @StoreDAO(target = "bc_bdb", daoClass = classOf[ODSDPoSVoteDao])
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

  @ActorRequire(name = "BlockChain_Helper", scope = "global")
  var bcHelper: BlockChainHelper = null;

  @ActorRequire(name = "Block_Helper", scope = "global")
  var blkHelper: BlockHelper = null;
  
  @ActorRequire(name = "Transaction_Helper", scope = "global")
  var txHelper: TransactionHelper = null;

  @ActorRequire(name = "bc_encoder", scope = "global") //  @BeanProperty
  var enc: EncAPI = null;
    
  def setPzp(_pzp: PZPCtrl) = {
    pzp = _pzp;
    Daos.pzp = pzp;
  }
  def getPzp(): PZPCtrl = {
    pzp
  }
  def setBcHelper(_bcHelper: BlockChainHelper) = {
    bcHelper = _bcHelper;
    Daos.actdb = bcHelper;
  }
  def getBcHelper: BlockChainHelper = {
    bcHelper
  }

  def setBlkHelper(_blkHelper: BlockHelper) = {
    blkHelper = _blkHelper;
    Daos.blkHelper = _blkHelper;
  }
  def getBlkHelper: BlockHelper = {
    blkHelper
  }
  
  def setTxHelper(_txHelper: TransactionHelper) = {
    txHelper = _txHelper;
    Daos.txHelper = _txHelper;
  }
  def getTxHelper: TransactionHelper = {
    txHelper
  }
  
  def setEnc(_enc: EncAPI) = {
    enc = _enc;
    Daos.enc = _enc;
  }
  def getEnc(): EncAPI = {
    enc;
  }
  
}

object Daos extends OLog {
  var dpospropdb: ODBSupport = null
  var dposvotedb:ODBSupport = null
  //  var blkdb: ODBSupport = null
  var pzp: PZPCtrl = null;
  var actdb: BlockChainHelper = null; 
  var blkHelper: BlockHelper = null;
  var txHelper: TransactionHelper = null;
  var enc: EncAPI = null;
  def isDbReady(): Boolean = {
    dpospropdb != null && dpospropdb.getDaosupport.isInstanceOf[ODBSupport] &&
    dposvotedb != null && dposvotedb.getDaosupport.isInstanceOf[ODBSupport] &&
      blkHelper != null &&
      txHelper != null &&
      pzp != null && actdb != null;
  }
}



