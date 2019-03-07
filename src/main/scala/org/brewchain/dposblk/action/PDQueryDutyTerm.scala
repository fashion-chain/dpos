package org.brewchain.dposblk.action

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
import org.brewchain.dposblk.pbgens.Dposblock.PSDutyTermVote
import org.brewchain.dposblk.pbgens.Dposblock.PDutyTermResult
import org.brewchain.dposblk.tasks.DCtrl
import onight.tfw.otransio.api.PacketHelper
import org.apache.commons.lang3.StringUtils
import org.fc.brewchain.bcapi.exception.FBSException
import org.brewchain.dposblk.pbgens.Dposblock.PDutyTermResult.VoteResult
import org.brewchain.dposblk.Daos
import org.brewchain.bcapi.gens.Oentity.OValue
import org.brewchain.bcapi.gens.Oentity.OKey
import com.google.protobuf.ByteString
import org.brewchain.account.util.IDGenerator
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
      log.error("query vote.");
      network.wallOutsideMessage("QDTDOB", Left(DCtrl.termMiner().setMessageId(UUIDGenerator.generate())
          .setVotebcuid(network.root().bcuid).build()), IDGenerator.nextID());
    }
  }

  //  override def getCmds(): Array[String] = Array(PWCommand.LST.name())
  override def cmd: String = PCommand.QDT.name();
}
