
package org.fok.dpos.task

import org.fok.p22p.node.Network
import org.fok.p22p.utils.LogHelper

import onight.tfw.outils.serialize.UUIDGenerator
import onight.tfw.async.CallBack
import onight.tfw.otransio.api.beans.FramePacket
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import org.fok.core.crypto.BitMap
import org.fok.dpos.model.Dposblock.PSCoMineOrBuilder
import org.fok.dpos.model.Dposblock.PSCoMine
import org.fok.dpos.model.Dposblock.PDNodeOrBuilder
import org.fok.dpos.model.Dposblock.PRetCoMine
import org.fok.dpos.util.DConfig
import org.fok.dpos.model.Dposblock.DNodeState
import org.fok.dpos.model.Dposblock.PSCoinbase
import org.fok.dpos.Daos
import org.fok.dpos.model.Dposblock.PBlockEntry
import org.fok.tools.bytes.BytesHelper
import org.apache.commons.codec.binary.Base64
import com.googlecode.protobuf.format.util.HexUtils
import org.apache.commons.codec.binary.Hex
import org.fok.tools.time.JodaTimeHelper
import scala.collection.JavaConversions._
import org.fok.dpos.util.BlkTxCalc
import org.fok.dpos.util.TxCache

object DTask_MineBlock extends LogHelper with BitMap {
  def runOnce(implicit network: Network): Boolean = {
    this.synchronized {
      //    Thread.currentThread().setName("RTask_MineBlock");
      val msgid = UUIDGenerator.generate();
      val cn = DCtrl.instance.cur_dnode;
      val curtime = System.currentTimeMillis();
      val (isMyBlock, isOverride) = DCtrl.checkMiner(Daos.actdb.getMaxConnectHeight.intValue() + 1, cn.getCoAddress, curtime,
        Thread.currentThread().getName, DConfig.BLK_EPOCH_MS);
      log.debug("checkMiner " + (Daos.actdb.getMaxConnectHeight.intValue() + 1) + ",co=" + cn.getCoAddress + " --> isMyBlock::" + isMyBlock + " isOverride::" + isOverride)

      if (isOverride) {
        //try to vote...
        val missedMinerCoaddr = DCtrl.minerByBlockHeight(Daos.actdb.getMaxConnectHeight.intValue() + 1).getOrElse("")
        var cc = 1;
        while (cc <= DConfig.DTV_BLOCKS_EACH_MINER + 2) {
          DCtrl.minerByBlockHeight(Daos.actdb.getMaxConnectHeight.intValue() + cc) match {
            case Some(coaddr) =>
              if (cn.getCoAddress.equals(coaddr) || isMyBlock) {
                DTask_DutyTermVote.VoteTerm(network, coaddr, cn.getCurBlock + 1)
                var dropuid = "";
                DCtrl.coMinerByUID.map(f => {
                  if (f._2.getCoAddress.equals(missedMinerCoaddr)) {
                    dropuid = f._1;
                  }
                })
                log.info("Drop CoMiner to Timeout overrided:" + dropuid);
                DCtrl.coMinerByUID.remove(dropuid);
                cc = DConfig.DTV_BLOCKS_EACH_MINER + 100;
                DCtrl.instance.cur_dnode.setState(DNodeState.DN_CO_MINER);
              } else if (!missedMinerCoaddr.equals(coaddr)) {
                DCtrl.instance.cur_dnode.setState(DNodeState.DN_CO_MINER);
                cc = DConfig.DTV_BLOCKS_EACH_MINER + 100;
              }
            case _ =>
          }
          cc = cc + 1;
        }

        false;
      } else if (isMyBlock) {
        MDCSetBCUID(network)
        val lastBlkTime = if (cn.getCurBlock == 0) 0 else Daos.actdb.getMaxConnectBlock.getHeader.getTimestamp;
        if (Daos.txHelper.getTmConfirmQueue.getTmConfirmQueue.size() == 0 //why?
          && (System.currentTimeMillis() - lastBlkTime) <
          Math.min(DConfig.BLK_NOOP_EPOCH_MS, DConfig.MAX_WAIT_BLK_EPOCH_MS * 2 / 3)) {
          log.debug("My Miner term LOOP: ch=" + cn.getCurBlock + ",past=" + JodaTimeHelper.secondFromNow(lastBlkTime)
            + "s,loopms=" + DConfig.BLK_NOOP_EPOCH_MS);
          false
        } else {
          val confirmTimes =
            if (DCtrl.curDN().getCurBlock == DCtrl.termMiner().getBlockRange.getEndBlock - 1) {
              DCtrl.termMiner().getCoNodes //最后一个要100%的确认
            } else {
              (DCtrl.termMiner().getCoNodes * DConfig.CREATE_BLOCK_TX_CONFIRM_PERCENT / 100).asInstanceOf[Int]
            }

          val start = System.currentTimeMillis();
          val (newblk, txs) = DCtrl.createNewBlock(
            //              DCtrl.termMiner().getMaxTnxEachBlock
            BlkTxCalc.getBestBlockTxCount(DCtrl.termMiner().getMaxTnxEachBlock), confirmTimes);

          if (newblk == null) {
            log.debug("mining error: ch=" + cn.getCurBlock);
            false;
          } else {
            val newblockheight = newblk.getHeader.getHeight.intValue()
            val newCoinbase = PSCoinbase.newBuilder()
              .setBlockHeight(newblockheight).setCoAddress(cn.getCoAddress)
              .setTermId(DCtrl.termMiner().getTermId)
              .setCoNodes(DCtrl.coMinerByUID.size)
              .setTermSign(DCtrl.termMiner().getSign)
              .setCoAddress(cn.getCoAddress)
              .setMineTime(curtime)
              .setMessageId(msgid)
              .setBcuid(cn.getBcuid)
              .setBlockEntry(PBlockEntry.newBuilder().setBlockHeight(newblockheight)
                .setCoinbaseBcuid(cn.getBcuid).setSliceId(DCtrl.termMiner().getSliceId)
                .setBlockHeader(newblk.toBuilder().clearBody().build().toByteString())
                //.setBlockMiner(newblk)
                .setSign(Daos.enc.bytesToHexStr(newblk.getHeader.getHash.toByteArray())))
              .setSliceId(DCtrl.termMiner().getSliceId)
              .setTxcount(txs.size())

            cn.setLastDutyTime(System.currentTimeMillis());
            cn.setCurBlock(newblockheight)
            DCtrl.instance.syncToDB()
            if (System.currentTimeMillis() - start > DConfig.ADJUST_BLOCK_TX_MAX_TIMEMS) {
              for (i <- 1 to 2) {
                BlkTxCalc.adjustTx(System.currentTimeMillis() - start)
              }
            } else {
              BlkTxCalc.adjustTx(System.currentTimeMillis() - start)
            }
            // write to 
            //send to next block.
            if (newblk.getHeader.getHeight < DCtrl.termMiner().getBlockRange.getEndBlock && txs.size() > 0) {
              // not the last one
              val nextMinerId = (newblk.getHeader.getHeight + 1 - DCtrl.termMiner().getBlockRange.getStartBlock).asInstanceOf[Int];
              val nextMiner = DCtrl.termMiner().getMinerQueue(nextMinerId);
              if (!nextMiner.getMinerCoaddr.equals(DCtrl.curDN().getCoAddress)) {
                // not the same node.
                DCtrl.coMinerByUID.map { cn =>
                  if (cn._2.getCoAddress.equals(nextMiner.getMinerCoaddr)) {
                    txs.map { tx =>
                      if (!Daos.txHelper.getTmConfirmQueue.containsConfirm(Daos.enc.bytesToHexStr(tx.getHash.toByteArray()), cn._2.getBitIdx)) {
                        newCoinbase.addTxbodies(tx.toByteString())
                      }
                    }
                    if (newCoinbase.getTxbodiesCount > 0) {
                      log.debug("send coinbbase with txbbody for " + cn._2.getBcuid + ",bitidx=" + cn._2.getBitIdx
                        + ",txcount=" + newCoinbase.getTxbodiesCount + "/" + txs.size() + ",block=" + newblk.getHeader.getHeight);
                      network.postMessage("MINDOB", Left(newCoinbase.build()),
                        UUIDGenerator.generate(), cn._2.getBcuid, '9')
                      newCoinbase.clearTxbodies();
                    }

                  }
                }
              }
            }
            network.wallMessage("MINDOB", Left(newCoinbase.build()), msgid, '9')
            TxCache.cacheTxs(txs);
            true
          }
        }
      } else {
        false
      }
    }
  }

}
