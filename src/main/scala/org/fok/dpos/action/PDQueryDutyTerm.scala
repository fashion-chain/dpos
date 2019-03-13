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
import org.fok.dpos.model.Dposblock.PSCoMine
import org.fok.dpos.model.Dposblock.PRetCoMine
import org.fok.dpos.model.Dposblock.PCommand
import org.fok.dpos.model.Dposblock.PSDutyTermVote
import org.fok.dpos.model.Dposblock.PDutyTermResult
import org.fok.dpos.task.DCtrl
import onight.tfw.otransio.api.PacketHelper
import org.apache.commons.lang3.StringUtils
import org.fok.p22p.exception.FBSException
import org.fok.dpos.model.Dposblock.PDutyTermResult.VoteResult
import org.fok.dpos.Daos
import com.google.protobuf.ByteString
import onight.tfw.outils.serialize.UUIDGenerator

@NActorProvider
@Instantiate
@Provides(specifications = Array(classOf[ActorService], classOf[IActor], classOf[CMDService]))
class PDQueryDutyTerm extends PSMDPoSNet[PSDutyTermVote] {
  override def service = PDQueryDutyTermService
}

//
// http://localhost:8000/fbs/xdn/pbget.do?bd=
object PDQueryDutyTermService extends LogHelper with PBUtils with LService[PSDutyTermVote] with PMNodeHelper {
  override def onPBPacket(pack: FramePacket, pbo: PSDutyTermVote, handler: CompleteHandler) = {
    //    log.debug("DPoS DutyTermResult::" + pack.getFrom())
    var ret = PDutyTermResult.newBuilder();
    val network = DCtrl.instance.network;
    if (!DCtrl.isReady() || network == null) {
      ret.setRetCode(-1).setRetMessage("DPoS Network Not READY")
      handler.onFinished(PacketHelper.toPBReturn(pack, ret.build()))
    } else {
      try {
        MDCSetBCUID(DCtrl.dposNet())
        val messageid=if(pbo.getMessageId==null)UUIDGenerator.generate() else pbo.getMessageId;
        MDCSetMessageID(messageid)
        ret.setMessageId(messageid);
        ret.setRetCode(0).setRetMessage("SUCCESS")
        val cn = DCtrl.curDN()
        val vq = DCtrl.voteRequest();

        //
        network.postMessage("PVRDOB", Left(DCtrl.termMiner().setVotebcuid(cn.getBcuid).build()),
          messageid,
          pbo.getVotebcuid);

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

  var lastUpdateTime:Long = 0L;
  def queryVote(): Unit = {
    val network = DCtrl.instance.network;
    if (!DCtrl.isReady() || network == null) {
      log.debug("DPoS Network Not READY")
    } else if (System.currentTimeMillis() - lastUpdateTime > 60 * 1000) {
      lastUpdateTime = System.currentTimeMillis();
      network.wallOutsideMessage("QDTDOB", Left(DCtrl.termMiner().setMessageId(UUIDGenerator.generate())
          .setVotebcuid(network.root().bcuid).build()), UUIDGenerator.generate());
    }
  }

  //  override def getCmds(): Array[String] = Array(PWCommand.LST.name())
  override def cmd: String = PCommand.QDT.name();
}
