package org.fok.dpos.action


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
import org.fok.dpos.model.Dposblock.PSNodeInfo
import onight.tfw.otransio.api.PacketHelper
import org.fok.p22p.exception.FBSException
import org.fok.dpos.model.Dposblock.PCommand
import org.fok.dpos.model.Dposblock.PRetNodeInfo
import org.fok.dpos.task.DCtrl
import scala.collection.JavaConversions._
import org.fok.dpos.model.Dposblock.PDNode

@NActorProvider
@Instantiate
@Provides(specifications = Array(classOf[ActorService], classOf[IActor], classOf[CMDService]))
class PDPoSNodeInfo extends PSMDPoSNet[PSNodeInfo] {
  override def service = PDPoSNodeInfoService
}

//
// http://localhost:8000/fbs/xdn/pbget.do?bd=
object PDPoSNodeInfoService extends LogHelper with PBUtils with LService[PSNodeInfo] with PMNodeHelper {
  override def onPBPacket(pack: FramePacket, pbo: PSNodeInfo, handler: CompleteHandler) = {
    log.debug("onPBPacket::" + pbo)
    var ret = PRetNodeInfo.newBuilder();
    val network = networkByID("dpos")
    if (network == null) {
      ret.setRetCode(-1).setRetMessage("unknow network:")
      handler.onFinished(PacketHelper.toPBReturn(pack, ret.build()))
    } else {
      try {
        MDCSetBCUID(network);
        //       pbo.getMyInfo.getNodeName
        ret.setDn(DCtrl.curDN())
        ret.setQueueBsbatch(PDPoSTransactionSyncService.dbBatchSaveList.size())
        ret.setQueueConfirm(PDPoSTransactionSyncService.confirmHashList.size())
        ret.setQueueWall(PDPoSTransactionSyncService.wallHashList.size())
//        DCtrl.termMiner().getMinerQueueList.map { tm =>
//          log.debug("termMiner==" + tm)
//          ret.addCoNodes(PDNode.newBuilder().setCoAddress(tm.getMinerCoaddr))
//        }
        DCtrl.coMinerByUID.map(kvs=>{
          val pn = kvs._2
          ret.addBackNodes(pn)
        })
      } catch {
        case e: FBSException => {
          ret.clear()
          ret.setRetCode(-2).setRetMessage(e.getMessage)
        }
        case t: Throwable => {
          log.error("error:", t);
          ret.clear()
          ret.setRetCode(-3).setRetMessage(t.getMessage)
        }
      } finally {
        handler.onFinished(PacketHelper.toPBReturn(pack, ret.build()))
      }
    }
  }
  //  override def getCmds(): Array[String] = Array(PWCommand.LST.name())
  override def cmd: String = PCommand.INF.name();
}
