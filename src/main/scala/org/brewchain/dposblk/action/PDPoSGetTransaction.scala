package org.brewchain.dposblk.action

import org.apache.commons.codec.binary.Hex
import org.apache.felix.ipojo.annotations.Instantiate
import org.apache.felix.ipojo.annotations.Provides
import onight.tfw.ntrans.api.ActorService
import onight.tfw.proxy.IActor
import onight.tfw.otransio.api.session.CMDService
import onight.osgi.annotation.NActorProvider
import org.brewchain.dposblk.PSMDPoSNet
import org.fc.brewchain.p22p.utils.LogHelper
import onight.oapi.scala.commons.PBUtils
import onight.oapi.scala.commons.LService
import org.fc.brewchain.p22p.action.PMNodeHelper
import onight.tfw.otransio.api.beans.FramePacket
import onight.tfw.async.CompleteHandler
import org.brewchain.bcapi.utils.PacketIMHelper._
import org.brewchain.dposblk.pbgens.Dposblock.PSCoMine
import org.brewchain.dposblk.pbgens.Dposblock.PRetCoMine
import org.brewchain.dposblk.pbgens.Dposblock.PCommand
import org.brewchain.dposblk.pbgens.Dposblock.PSGetTransaction
import org.brewchain.dposblk.pbgens.Dposblock.PRetGetTransaction
import org.brewchain.dposblk.tasks.DCtrl
import onight.tfw.otransio.api.PacketHelper
import org.fc.brewchain.bcapi.exception.FBSException
import org.brewchain.dposblk.Daos
import org.brewchain.evmapi.gens.Tx.MultiTransaction
import com.google.protobuf.ByteString;

import scala.collection.JavaConversions._
import onight.tfw.otransio.api.PackHeader
import org.brewchain.dposblk.utils.TxCache

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
        log.error("get txbyHash=" + pbo.getTxHashCount + ",from=" + from + " ,node=" + DCtrl.curDN().getBcuid)
        var i = 0;
        for (txHash <- pbo.getTxHashList) {
          
          val tx =  TxCache.getTx(txHash) match{
            case t if t!=null =>
              t
            case _ =>
              val t=Daos.txHelper.GetTransaction(txHash)
              TxCache.recentBlkTx.put(txHash, t); 
              t;
          }
            
          if (tx != null) {
            ret.addTxContent(ByteString.copyFrom(tx.toByteArray()));
          } else {
            if (i < 10) {
              log.error("cannot get tx by hash:" + txHash + ",from=" + from + " ,node=" + DCtrl.curDN().getBcuid);
            }
            i = i + 1;
          }
        }
        ret.setRetCode(1)
        log.error("return gettx cost=" + (System.currentTimeMillis() - start) + ",txcount=" + pbo.getTxHashCount);

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