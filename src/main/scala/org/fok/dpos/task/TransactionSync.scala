package org.fok.dpos.task

import org.fok.p22p.node.Network
import org.fok.p22p.utils.LogHelper
import onight.tfw.outils.serialize.UUIDGenerator
import onight.tfw.async.CallBack
import onight.tfw.otransio.api.beans.FramePacket

import scala.collection.JavaConversions._
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.TimeUnit
import org.fok.dpos.util.DConfig
import org.fok.p22p.node.Networks
import java.util.concurrent.atomic.AtomicBoolean
import org.fok.dpos.model.Dposblock.PSSyncTransaction
import org.fok.dpos.Daos
import org.fok.dpos.util.SRunner
import org.fok.p22p.action.PMNodeHelper
import org.fok.dpos.model.Dposblock.PSSyncTransaction.SyncType
import java.math.BigInteger
import java.util.concurrent.atomic.AtomicInteger
import com.google.protobuf.ByteString

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
      val res = Daos.txHelper.getTmMessageQueue.poll(DConfig.MAX_TNX_EACH_BROADCAST)
      if (res.size() > 0) {
        val msgid = UUIDGenerator.generate();
        val syncTransaction = PSSyncTransaction.newBuilder();
        syncTransaction.setMessageid(msgid);
        syncTransaction.setSyncType(SyncType.ST_WALLOUT);
        syncTransaction.setFromBcuid(network.root().bcuid);
        for (x <- res) {
          syncTransaction.addTxHash(ByteString.copyFrom(x.getKey))
           syncTransaction.addTxDatas(ByteString.copyFrom(x.getTx.toByteArray()))
        }

        network.wallMessage("BRTDOB", Left(syncTransaction.build()), msgid)
        lastSyncTime.set(startTime)
        lastSyncCount.set(res.size())
      } else {
      }
    }
  }
}