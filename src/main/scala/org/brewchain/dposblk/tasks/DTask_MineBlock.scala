
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
import org.brewchain.dposblk.pbgens.Dposblock.PSCoinbase
import org.brewchain.dposblk.Daos
import org.brewchain.dposblk.pbgens.Dposblock.PBlockEntry
import org.brewchain.account.util.ByteUtil
import org.apache.commons.codec.binary.Base64
import com.googlecode.protobuf.format.util.HexUtils
import org.apache.commons.codec.binary.Hex
import org.fc.brewchain.bcapi.JodaTimeHelper
import scala.collection.JavaConversions._
import org.brewchain.dposblk.utils.BlkTxCalc
import org.brewchain.dposblk.utils.TxCache

//获取其他节点的term和logidx，commitidx
object DTask_MineBlock extends LogHelper with BitMap {
  def runOnce(implicit network: Network): Boolean = {
    this.synchronized {
      //    Thread.currentThread().setName("RTask_MineBlock");
      val msgid = UUIDGenerator.generate();
      val cn = DCtrl.instance.cur_dnode;
      val curtime = System.currentTimeMillis();
      //      log.debug("checkMiner --> call cn::" + cn);
      //      val (isMyBlock, isOverride) = DCtrl.checkMiner(cn.getCurBlock + 1, cn.getCoAddress, curtime,
      //        Thread.currentThread().getName ,DConfig.BLK_EPOCH_MS);

      val (isMyBlock, isOverride) = DCtrl.checkMiner(Daos.actdb.getLastBlockNumber.intValue() + 1, cn.getCoAddress, curtime,
        Thread.currentThread().getName, DConfig.BLK_EPOCH_MS);
      log.debug("checkMiner " + (Daos.actdb.getLastBlockNumber.intValue() + 1) + ",co=" + cn.getCoAddress + " --> isMyBlock::" + isMyBlock + " isOverride::" + isOverride)

      if (isOverride) {
        //try to vote...
        //
        DCtrl.minerByBlockHeight(cn.getCurBlock + 1) match {
          case Some(coaddr) =>
            if (isMyBlock) {
              DTask_DutyTermVote.VoteTerm(network, coaddr, cn.getCurBlock + 1)
            }
            DCtrl.instance.cur_dnode.setState(DNodeState.DN_CO_MINER);
          case None =>
        }

        false;
      } else if (isMyBlock) {
        MDCSetBCUID(network)
        val lastBlkTime = if (cn.getCurBlock == 0) 0 else Daos.blkHelper.GetBestBlock().getHeader.getTimestamp;
        if (Daos.txHelper.getOConfirmMapDB.size() == 0 //why?
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
            val newblockheight = newblk.getHeader.getNumber.intValue()
            //        log.debug("MineNewBlock:" + newblk);
            log.debug("mining check ok :new block=" + newblockheight + ",CO=" + cn.getCoAddress
              + ",MaxTnx=" + DCtrl.termMiner().getMaxTnxEachBlock + ",hash=" + newblk.getHeader.getBlockHash);
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
                .setBlockHeader(newblk.clearBody().build().toByteString())
                //.setBlockMiner(newblk)
                .setSign(newblk.getHeader.getBlockHash))
              .setSliceId(DCtrl.termMiner().getSliceId)
              .setTxcount(txs.size())

            //          log.debug("TRACE::BLKSH=["+Base64.encodeBase64String(newCoinbase.getBlockHeader.getBlockHeader.toByteArray())+"]");

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
            if (newblk.getHeader.getNumber < DCtrl.termMiner().getBlockRange.getEndBlock && txs.size() > 0) {
              // not the last one
              val nextMinerId = (newblk.getHeader.getNumber + 1 - DCtrl.termMiner().getBlockRange.getStartBlock).asInstanceOf[Int];
              val nextMiner = DCtrl.termMiner().getMinerQueue(nextMinerId);
              if (!nextMiner.getMinerCoaddr.equals(DCtrl.curDN().getCoAddress)) {
                // not the same node.
                DCtrl.coMinerByUID.map { cn =>
                  if (cn._2.getCoAddress.equals(nextMiner.getMinerCoaddr)) {
                    txs.map { tx =>
                      if (!Daos.txHelper.containConfirm(tx.getTxHash, cn._2.getBitIdx)) {
                        newCoinbase.addTxbodies(tx.toByteString())
                      }
                    }
                    if (newCoinbase.getTxbodiesCount > 0) {
                      log.error("send coinbbase with txbbody for " + cn._2.getBcuid + ",bitidx=" + cn._2.getBitIdx
                        + ",txcount=" + newCoinbase.getTxbodiesCount + "/" + txs.size() + ",block=" + newblk.getHeader.getNumber);
                      network.postMessage("MINDOB", Left(newCoinbase.build()),
                        UUIDGenerator.generate(), cn._2.getBcuid, '9')
                      newCoinbase.clearTxbodies();
                    }

                  }
                }
              }
            }
            //
            //            log.debug("mindob newblockheight::" + newblockheight + " cn.getCoAddress::" + cn.getCoAddress + " termid::" + DCtrl.termMiner().getTermId + " cn.getBcuid::" + cn.getBcuid)
            network.wallMessage("MINDOB", Left(newCoinbase.build()), msgid, '9')
            TxCache.cacheTxs(txs);
            true
          }
        }
      } else {

        log.debug("waiting for my mine block:" + (cn.getCurBlock + 1) + ",CO=" + cn.getCoAddress
          + ",sign=" + DCtrl.termMiner().getSign);
        false
      }
    }
  }

}
