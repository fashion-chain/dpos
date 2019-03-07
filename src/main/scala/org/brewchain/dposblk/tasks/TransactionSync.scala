package org.brewchain.dposblk.tasks

import org.fc.brewchain.p22p.node.Network
import org.fc.brewchain.p22p.utils.LogHelper
import onight.tfw.outils.serialize.UUIDGenerator
import onight.tfw.async.CallBack
import onight.tfw.otransio.api.beans.FramePacket

import scala.collection.JavaConversions._
import org.brewchain.bcapi.gens.Oentity.OValue
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.TimeUnit
import org.brewchain.dposblk.utils.DConfig
import org.fc.brewchain.p22p.node.Networks
import java.util.concurrent.atomic.AtomicBoolean
import org.brewchain.dposblk.pbgens.Dposblock.PSSyncTransaction
import org.brewchain.dposblk.Daos
import org.brewchain.bcapi.exec.SRunner
import org.fc.brewchain.p22p.action.PMNodeHelper
import org.brewchain.dposblk.pbgens.Dposblock.PSSyncTransaction.SyncType
import java.math.BigInteger
import java.util.concurrent.atomic.AtomicInteger

case class TransactionSync(network: Network) extends SRunner with PMNodeHelper with LogHelper {
  def getName() = "TxSync"
  def runOnce() = {
    Thread.currentThread().setName("TxSync");
    TxSync.trySyncTx(network);
  }
}
object TxSync extends LogHelper {
  var instance: TransactionSync = TransactionSync(null);
  def dposNet(): Network = instance.network;
  val lastSyncTime = new AtomicLong(0);
  val lastSyncCount = new AtomicInteger(0);

  def isLimitSyncSpeed(curTime: Long): Boolean = {
    val tps = lastSyncCount.get * 1000 / (Math.abs((curTime - lastSyncTime.get)) + 1);
    if (tps > DConfig.SYNC_TX_TPS_LIMIT) {
      log.warn("speed limit :curTps=" + tps + ",timepass=" + (curTime - lastSyncTime.get) + ",lastSyncCount=" + lastSyncCount);
      true
    } else {
      false
    }

  }
  def trySyncTx(network: Network): Unit = {
    val startTime = System.currentTimeMillis();
    if (!isLimitSyncSpeed(startTime)) {
      val res = Daos.txHelper.getWaitSendTxToSend(DConfig.MAX_TNX_EACH_BROADCAST)
      if (res.getTxHashCount > 0) {
        log.debug("Try To Synctx:Count="+res.getTxHashCount);
        val msgid = UUIDGenerator.generate();
        val syncTransaction = PSSyncTransaction.newBuilder();
        syncTransaction.setMessageid(msgid);
        syncTransaction.setSyncType(SyncType.ST_WALLOUT);
        syncTransaction.setFromBcuid(network.root().bcuid);
        for (x <- res.getTxHashList) {
          syncTransaction.addTxHash(x)
        }

        for (x <- res.getTxDatasList) {
          syncTransaction.addTxDatas(x)
        }

        //      syncTransaction.addAllTxHash(res.getTxHashList);
        //      syncTransaction.addAllTxDatas(res.getTxDatasList);
        network.wallMessage("BRTDOB", Left(syncTransaction.build()), msgid)
        lastSyncTime.set(startTime)
        lastSyncCount.set(res.getTxHashCount)
      } else {
      }
    }
  }
}