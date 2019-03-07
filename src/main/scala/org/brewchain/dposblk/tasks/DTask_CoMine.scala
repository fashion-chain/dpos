
package org.brewchain.dposblk.tasks

import org.fc.brewchain.p22p.node.Network
import org.fc.brewchain.p22p.utils.LogHelper

import onight.tfw.outils.serialize.UUIDGenerator
import onight.tfw.async.CallBack
import onight.tfw.otransio.api.beans.FramePacket
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import org.fc.brewchain.bcapi.crypto.BitMap
import org.brewchain.dposblk.pbgens.Dposblock.PSCoMineOrBuilder
import org.brewchain.dposblk.pbgens.Dposblock.PSCoMine
import org.brewchain.dposblk.pbgens.Dposblock.PDNodeOrBuilder
import org.brewchain.dposblk.pbgens.Dposblock.PRetCoMine
import org.brewchain.dposblk.utils.DConfig
import org.brewchain.dposblk.pbgens.Dposblock.DNodeState
import org.apache.commons.lang3.StringUtils
import scala.collection.JavaConversions._
//获取其他节点的term和logidx，commitidx
object DTask_CoMine extends LogHelper with BitMap {
  def runOnce(implicit network: Network): PDNodeOrBuilder = {
    Thread.currentThread().setName("DTask_Join");
    val tm = DCtrl.termMiner();
    if (tm.getTermId > 0 && tm.getBlockRange != null) {
      DCtrl.curDN().setTermId(tm.getTermId).setTermStartBlock(tm.getBlockRange.getStartBlock)
        .setTermEndBlock(tm.getBlockRange.getEndBlock).setTermSign(tm.getSign).setBitIdx(DCtrl.dposNet().root().node_idx)
    }
    val join = PSCoMine.newBuilder().setDn(DCtrl.curDN())
      .build();
    val msgid = UUIDGenerator.generate();
    val cn = DCtrl.instance.cur_dnode;
    var fastNode: PDNodeOrBuilder = cn;
    var minCost: Long = Long.MaxValue;
    var maxBlockHeight: Long = 0;
    MDCSetBCUID(network)

    val cdl = new CountDownLatch(network.directNodes.size)
    network.directNodes //.filter { n => !DCtrl.coMinerByUID.contains(n.bcuid) }
      .map { n =>
        val start = System.currentTimeMillis();
        network.asendMessage("JINDOB", join, n, new CallBack[FramePacket] {
          def onSuccess(fp: FramePacket) = {
            try {
              val end = System.currentTimeMillis();
              val retjoin = if (fp.getBody != null) {
                PRetCoMine.newBuilder().mergeFrom(fp.getBody);
              } else if (fp.getFbody != null && fp.getFbody.isInstanceOf[PRetCoMine]) {
                fp.getFbody.asInstanceOf[PRetCoMine]
              } else {
                null;
              }
              MDCSetBCUID(network)
              if (retjoin != null && retjoin.getRetCode() == 0) { //same message
                log.debug("send JINDOB success:to " + n.uri + ",code=" + retjoin.getRetCode)
                if (fastNode == null) {
                  fastNode = retjoin.getDn;
                } else if (retjoin.getDn.getCurBlock > fastNode.getCurBlock) {
                  fastNode = retjoin.getDn;
                  minCost = end - start
                } else if (retjoin.getDn.getCurBlock == fastNode.getCurBlock) {
                  if (end - start < minCost) { //set the fast node
                    minCost = end - start
                    fastNode = retjoin.getDn;
                  }
                }
                log.debug("get other nodeInfo:B=" + retjoin.getDn.getCurBlock + ",state=" + retjoin.getCoResult + ",bcuid=" + retjoin.getDn.getBcuid);
                if (retjoin.getCoResult == DNodeState.DN_CO_MINER) {
                  DCtrl.coMinerByUID.put(retjoin.getDn.getBcuid, retjoin.getDn);
                }
              } else {
                log.debug("send JINDOB Failed " + n.uri + ",retobj=" + retjoin)
              }
            } finally {
              cdl.countDown()
            }
          }
          def onFailed(e: java.lang.Exception, fp: FramePacket) {
            cdl.countDown()
            log.debug("send JINDOB ERROR " + n.uri + ",e=" + e.getMessage, e)
          }
        })
      }
    cdl.await();
    //    log.debug("get nodes:count=" + DCtrl.coMinerByUID.size + ",dposnetNodecount=" + network.directNodeByBcuid.size
    //      + ",maxheight=" + fastNode.getCurBlock);
    //remove off line
    DCtrl.coMinerByUID.filter(p => {
      network.nodeByBcuid(p._1) == network.noneNode
    }).map { p =>
      log.debug("remove Node:" + p._1);
      DCtrl.coMinerByUID.remove(p._1);
    }
    if (cn.getCurBlock + DConfig.BLOCK_DISTANCE_COMINE + 1 < DCtrl.termMiner().getBlockRange.getStartBlock
      && System.currentTimeMillis() < DCtrl.termMiner().getTermEndMs + DConfig.DTV_TIMEOUT_SEC * 1000) {
      log.debug("TermBlock large than local:T=[" + DCtrl.termMiner().getBlockRange.getStartBlock + "," + DCtrl.termMiner().getBlockRange.getEndBlock +
        "],DB=" + cn.getCurBlock);
      cn.setState(DNodeState.DN_SYNC_BLOCK)
      if (fastNode == null || StringUtils.equals(fastNode.getBcuid, cn.getBcuid)) {
        fastNode = null;
        DCtrl.coMinerByUID.filter(p => {
          network.nodeByBcuid(p._1) == network.noneNode &&
            !StringUtils.equals(p._1, cn.getBcuid)
        }).map(p => {
          if (fastNode == null || p._2.getCurBlock > fastNode.getCurBlock) {
            fastNode = p._2;
          }
        })
        if (fastNode != null) {
          BlockSync.trySyncBlock(fastNode.getCurBlock, fastNode.getBcuid);
          log.debug("get from other gcuid:" + fastNode.getBcuid);
        } else {
          log.debug("wait from other nodes online.");
        }

        fastNode
      } else {
        BlockSync.trySyncBlock(fastNode.getCurBlock, fastNode.getBcuid);
        fastNode
      }
    } else if (fastNode != cn && cn.getCurBlock < fastNode.getCurBlock) {
      cn.setState(DNodeState.DN_SYNC_BLOCK)
      BlockSync.trySyncBlock(fastNode.getCurBlock, fastNode.getBcuid);
      fastNode
    } else if (DConfig.RUN_COMINER == 1 && cn.getCurBlock >= fastNode.getCurBlock
      && DCtrl.coMinerByUID.size > 0 && DCtrl.coMinerByUID.size >= network.directNodes.size * 2 / 3) {
      if (cn.getCurBlock >= tm.getBlockRange.getStartBlock &&
        cn.getCurBlock <= tm.getBlockRange.getEndBlock && tm.getBlockRange.getEndBlock > 1) {
        cn.setState(DNodeState.DN_CO_MINER)
        tm.getMinerQueueList.map { f =>
          if (cn.getState != DNodeState.DN_DUTY_MINER && f.getMinerCoaddr.equals(cn.getCoAddress)) {
            cn.setState(DNodeState.DN_DUTY_MINER);
            log.debug("recover to become duty miner is max:cur=" + cn.getCurBlock + ", net=" + fastNode.getCurBlock);
          }
        }
      } else {
        log.debug("ready to become cominer is max:cur=" + cn.getCurBlock + ", net=" + fastNode.getCurBlock);
        cn.setState(DNodeState.DN_CO_MINER)
      }
      if (DCtrl.coMinerByUID.size > 1) {
        cn.setCominerStartBlock(cn.getCurBlock);
      }
      cn
    } else {
      cn.setState(DNodeState.DN_SYNC_BLOCK)
      null
    }

  }

}
