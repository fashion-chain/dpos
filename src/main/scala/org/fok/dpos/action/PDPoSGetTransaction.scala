package org.fok.dpos.action

import org.apache.commons.codec.binary.Hex
import org.apache.felix.ipojo.annotations.Instantiate
import org.apache.felix.ipojo.annotations.Provides
import onight.tfw.ntrans.api.ActorService
import onight.tfw.proxy.IActor
import onight.tfw.otransio.api.session.CMDService
import onight.osgi.annotation.NActorProvider
import org.fok.dpos.PSMDPoSNet
import org.fok.p22p.utils.LogHelper
import onight.oapi.scala.commons.PBUtils
import onight.oapi.scala.commons.LService
import org.fok.p22p.action.PMNodeHelper
import onight.tfw.otransio.api.beans.FramePacket
import onight.tfw.async.CompleteHandler
import org.fok.p22p.utils.PacketIMHelper._
import org.fok.dpos.model.Dposblock.PSCoMine
import org.fok.dpos.model.Dposblock.PRetCoMine
import org.fok.dpos.model.Dposblock.PCommand
import org.fok.dpos.model.Dposblock.PSGetTransaction
import org.fok.dpos.model.Dposblock.PRetGetTransaction
import org.fok.dpos.task.DCtrl
import onight.tfw.otransio.api.PacketHelper
import org.fok.p22p.exception.FBSException
import org.fok.dpos.Daos
import org.fok.core.model.Transaction.TransactionInfo
import com.google.protobuf.ByteString;

import scala.collection.JavaConversions._
import onight.tfw.otransio.api.PackHeader
import org.fok.dpos.util.TxCache
import net.sf.ehcache.Element;

@NActorProvider
@Instantiate
@Provides(specifications = Array(classOf[ActorService], classOf[IActor], classOf[CMDService]))
class PDPoSGetTransaction extends PSMDPoSNet[PSGetTransaction] {
  override def service = PDPoSGetTransactionService
}

object PDPoSGetTransactionService extends LogHelper with PBUtils with LService[PSGetTransaction] with PMNodeHelper {
  override def onPBPacket(pack: FramePacket, pbo: PSGetTransaction, handler: CompleteHandler) = {
    var ret = PRetGetTransaction.newBuilder();
    if (!DCtrl.isReady()) {
      ret.setRetCode(-1).setRetMessage("DPoS Network Not READY")
      handler.onFinished(PacketHelper.toPBReturn(pack, ret.build()))
    } else {
      try {
        var start = System.currentTimeMillis();
        val from = pack.getExtStrProp(PackHeader.PACK_FROM)
//        log.error("get txbyHash=" + pbo.getTxHashCount + ",from=" + from + " ,node=" + DCtrl.curDN().getBcuid)
        var i = 0;
        for (txHash <- pbo.getTxHashList) {
          
          val tx =  TxCache.getTx(txHash.toByteArray()) match{
            case t if t!=null =>
              t
            case _ =>
              val t=Daos.txHelper.getTransaction(txHash.toByteArray())
              var element = new Element(txHash.toByteArray(), t);
              TxCache.recentBlkTx.put(element); 
              t;
          }
            
          if (tx != null) {
            ret.addTxContent(ByteString.copyFrom(tx.toByteArray()));
          } else {
            if (i < 10) {
              log.info("cannot get tx by hash:" + txHash + ",from=" + from + " ,node=" + DCtrl.curDN().getBcuid);
            }
            i = i + 1;
          }
        }
        ret.setRetCode(1)
//        log.info("return gettx cost=" + (System.currentTimeMillis() - start) + ",txcount=" + pbo.getTxHashCount);

      } catch {
        case t: Throwable => {
          log.error("error in GetTransaction:", t);
          ret.clear()
          ret.setRetCode(-3).setRetMessage(t.getMessage)
        }
      } finally {
        handler.onFinished(PacketHelper.toPBReturn(pack, ret.build()))
      }
    }
  }
  override def cmd: String = PCommand.SRT.name();
}