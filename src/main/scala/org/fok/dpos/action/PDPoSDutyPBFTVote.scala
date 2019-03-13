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
import org.fok.dpos.task.DCtrl
import onight.tfw.otransio.api.PacketHelper
import org.apache.commons.lang3.StringUtils
import org.fok.p22p.exception.FBSException
import org.fok.dpos.model.Dposblock.PDutyTermResult.VoteResult
import org.fok.dpos.Daos
import com.google.protobuf.ByteString
import java.util.concurrent.TimeUnit
import org.fok.p22p.core.Votes
import org.fok.p22p.core.Votes.Converge
import org.fok.p22p.core.Votes.Undecisible
import org.fok.dpos.model.Dposblock.PSDutyTermVote
import org.fok.dpos.model.Dposblock.PDutyTermResult
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import org.fok.dpos.model.Dposblock.PCommand
import onight.tfw.outils.serialize.UUIDGenerator
import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Element;
import net.sf.ehcache.store.MemoryStoreEvictionPolicy;

@NActorProvider
@Instantiate
@Provides(specifications = Array(classOf[ActorService], classOf[IActor], classOf[CMDService]))
class PDPoSDutyPBFTVote extends PSMDPoSNet[PSDutyTermVote] {
  override def service = PDPoSDutyPBFTVote
}

//
// http://localhost:8000/fbs/xdn/pbget.do?bd=
object PDPoSDutyPBFTVote extends LogHelper with PBUtils with LService[PSDutyTermVote] with PMNodeHelper {
//  val votingTerms: Cache[String, PSDutyTermVote] = CacheBuilder.newBuilder().expireAfterWrite(180, TimeUnit.SECONDS)
//    .maximumSize(512).build().asInstanceOf[Cache[String, PSDutyTermVote]]
    val votingTerms: Cache = new Cache("dpos_votingterms", 100, MemoryStoreEvictionPolicy.LRU,
    true, "./dpos_votingterms", true,
    0, 0, true, 120, null)
    
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
        val messageid = if (pbo.getMessageId == null) UUIDGenerator.generate() else pbo.getMessageId;

        MDCSetMessageID(messageid)
        ret.setMessageId(messageid);
        ret.setRetCode(0).setRetMessage("SUCCESS")
        val cn = DCtrl.curDN()
        val vq = DCtrl.voteRequest();
        
        var element = new Element(pbo.getVotebcuid, pbo)
        votingTerms.put(element);
        if (votingTerms.getSize() > pbo.getCoNodes * 2 / 3) {
          //try to udpate.
          val termlist = new ListBuffer[PSDutyTermVote]();
          for (t <- votingTerms.getKeys) {
            termlist.add( votingTerms.get(t).getValue.asInstanceOf[PSDutyTermVote]);
          }
          Votes.vote(termlist).PBFTVote({ p =>
            Some(p.getSign)
          }, pbo.getCoNodes) match {
            case Converge(n) =>
              var termMerge: PSDutyTermVote = null;
              
              votingTerms.getKeys.map(ks =>
                if (votingTerms.get(ks).getValue.asInstanceOf[PSDutyTermVote].getSign.equals(n)) {
                  termMerge = votingTerms.get(ks).getValue.asInstanceOf[PSDutyTermVote];
                })
              if (termMerge != null) {
                if (DCtrl.termMiner().getTermId == 0 || StringUtils.isBlank(DCtrl.termMiner().getSign)) {
                  log.error("get converge :" + termMerge.getSign + ",tid=" + termMerge.getTermId);
                  DCtrl.instance.term_Miner = termMerge.toBuilder()
                  DCtrl.instance.updateTerm()
                }
              }
            case _ =>
              log.info("cannot get converge for pbft vote:"+votingTerms);
          }

        }
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
  override def cmd: String = PCommand.PVR.name();
}
