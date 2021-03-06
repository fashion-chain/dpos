package org.fok.dpos.action

import org.apache.commons.codec.binary.Hex
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
import org.fok.dpos.model.Dposblock.PSSyncTransaction
import org.fok.dpos.model.Dposblock.PRetSyncTransaction
import org.fok.dpos.task.DCtrl
import onight.tfw.otransio.api.PacketHelper
import org.fok.p22p.exception.FBSException
import org.fok.dpos.Daos
import org.fok.core.model.Transaction.TransactionInfo

import scala.collection.JavaConversions._
import org.apache.commons.lang3.StringUtils
import java.util.ArrayList
import java.math.BigInteger
import org.fok.dpos.model.Dposblock.PSSyncTransaction.SyncType
import org.fok.core.bean.TransactionMessage
import onight.tfw.outils.serialize.UUIDGenerator
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.atomic.AtomicBoolean
import org.fok.dpos.task.Scheduler
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.TimeUnit
import com.google.protobuf.ByteString
import org.fok.dpos.util.DConfig
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.locks.ReentrantReadWriteLock
import java.util.concurrent.LinkedBlockingDeque
import org.fok.core.bean.TransactionMessageQueue
import lombok.Data
import org.fok.dpos.bean.PendingQueue

@NActorProvider
@Instantiate
@Provides(specifications = Array(classOf[ActorService], classOf[IActor], classOf[CMDService]))
class PDPoSTransactionSync extends PSMDPoSNet[PSSyncTransaction] {
  override def service = PDPoSTransactionSyncService
}

object PDPoSTransactionSyncService extends LogHelper with PBUtils with LService[PSSyncTransaction] with PMNodeHelper {
  val greendbBatchSaveList = new LinkedBlockingDeque[(ArrayList[TransactionInfo], BigInteger, CompleteHandler)]();
  val dbBatchSaveList = new PendingQueue[(Array[Byte], BigInteger)]("batchsavelist", 100);
  val confirmHashList = new LinkedBlockingQueue[(Array[Byte], BigInteger)]();

  val wallHashList = new LinkedBlockingQueue[ByteString]();

  val running = new AtomicBoolean(false);
  val prioritySave = new ReentrantReadWriteLock().writeLock();

  case class BatchRunner(id: Int) extends Runnable {
    def poll(): (ArrayList[TransactionInfo], BigInteger, CompleteHandler) = {
      val ret = greendbBatchSaveList.poll();
      if (ret != null) {
        ret;
      } else {
        val p = dbBatchSaveList.pollFirst();
        if (p != null) {
          val pbo = PSSyncTransaction.newBuilder().mergeFrom(p._1);
          val dbsaveList = new ArrayList[TransactionInfo]();
          for (x <- pbo.getTxDatasList) {
            var oMultiTransaction = TransactionInfo.parseFrom(x);
            if (!StringUtils.equals(DCtrl.curDN().getBcuid, oMultiTransaction.getNode.getNid)) {
              dbsaveList.add(oMultiTransaction)
            }
          }
          (dbsaveList, p._2, null)
        } else {
          null
        }
      }
    }
    override def run() {
      running.set(true);
      Thread.currentThread().setName("DPosTx-BatchRunner-" + id);
      while (running.get) {
        try {
          var p = poll();
          while (p != null) {
            Daos.txHelper.syncTransactionBatch(p._1, true, p._2);
            if (p._3 != null) {
              p._3.onFinished(null);
            }
            p._1.clear();
            p = null;
            //should sleep when too many tx to confirm.
            if (Daos.txHelper.getTmConfirmQueue.size() < Daos.txHelper.getTmConfirmQueue.getMaxElementsInMemory) {
              p = poll();
            }
          }
          if (p == null) {
            Thread.sleep(500);
          }
        } catch {
          case ier: IllegalStateException =>
            try {
              Thread.sleep(1000)
            } catch {
              case t: Throwable =>
            }
          case t: Throwable =>
            log.error("get error", t);
        } finally {
          try {
            Thread.sleep(10)
          } catch {
            case t: Throwable =>
          }
        }
      }
    }

  }

  case class ConfirmRunner(id: Int) extends Runnable {
    override def run() {
      running.set(true);
      Thread.currentThread().setName("DPosTx-ConfirmRunner-" + id);
      while (running.get) {
        try {
          var h = confirmHashList.poll(10, TimeUnit.SECONDS);
          while (h != null) {
            Daos.txHelper.syncTransactionConfirm(h._1, h._2);
            h = null;
            //should sleep when too many tx to confirm.
            if (Daos.txHelper.getTmConfirmQueue.size() < Daos.txHelper.getTmConfirmQueue.getMaxElementsInMemory) {
              h = confirmHashList.poll();
            }
          }
        } catch {
          case t: Throwable =>
            log.error("get error", t);
        } finally {
          try {
            Thread.sleep(100)
          } catch {
            case t: Throwable =>
          }
        }
      }
    }
  }

  case class WalloutRunner(id: Int) extends Runnable {
    override def run() {
      running.set(true);
      Thread.currentThread().setName("DPosTx-WalloutRunner-" + id);
      while (running.get) {
        try {
          var h = wallHashList.poll(10, TimeUnit.SECONDS);
          if (h != null) {
            val msgid = UUIDGenerator.generate();
            val syncTransaction = PSSyncTransaction.newBuilder();
            syncTransaction.setMessageid(msgid);
            syncTransaction.setSyncType(SyncType.ST_CONFIRM_RECV);
            syncTransaction.setFromBcuid(DCtrl.instance.network.root().bcuid);
            syncTransaction.setConfirmBcuid(DCtrl.instance.network.root().bcuid);
            while (h != null) {
              syncTransaction.addTxHash(h);
              h = null;
              if (syncTransaction.getTxHashCount < DConfig.MIN_TNX_EACH_BROADCAST) {
                h = wallHashList.poll(10, TimeUnit.MILLISECONDS);
              } else if (syncTransaction.getTxHashCount < DConfig.MAX_TNX_EACH_BROADCAST) {
                h = wallHashList.poll();
              }
            }
            if (syncTransaction.getTxHashCount > 0) {
              DCtrl.instance.network.wallMessage("BRTDOB", Left(syncTransaction.build()), msgid)
            }
          }
        } catch {
          case t: Throwable =>
            log.error("get error", t);
        } finally {
          try {
            Thread.sleep(10)
          } catch {
            case t: Throwable =>
          }
        }
      }
    }
  }
  for (i <- 1 to DConfig.PARALL_SYNC_TX_BATCHBS) {
    new Thread(new BatchRunner(i)).start()
  }
  for (i <- 1 to DConfig.PARALL_SYNC_TX_CONFIRM) {
    new Thread(new ConfirmRunner(i)).start()
  }
  for (i <- 1 to DConfig.PARALL_SYNC_TX_WALLOUT) {
    new Thread(new WalloutRunner(i)).start()
  }

  override def onPBPacket(pack: FramePacket, pbo: PSSyncTransaction, handler: CompleteHandler) = {
    var ret = PRetSyncTransaction.newBuilder();
    if (!DCtrl.isReady()) {
      ret.setRetCode(-1).setRetMessage("DPoS Network Not READY")
      handler.onFinished(PacketHelper.toPBReturn(pack, ret.build()))
    } else {
      try {
        MDCSetBCUID(DCtrl.dposNet())
        MDCSetMessageID(pbo.getMessageid)
        //        log.debug("OnSyncTx:" + pbo.getSyncType + ",count=" + pbo.getTxHashCount + ",from=" + pbo.getFromBcuid + ",confirm=" + pbo.getConfirmBcuid);

        var bits = BigInteger.ZERO.setBit(DCtrl.instance.network.root().node_idx);
        val confirmNode =
          pbo.getSyncType match {
            case SyncType.ST_WALLOUT =>
              DCtrl.instance.network.nodeByBcuid(pbo.getFromBcuid);
            case _ =>
              DCtrl.instance.network.nodeByBcuid(pbo.getConfirmBcuid);
          }

        if (confirmNode != DCtrl.instance.network.noneNode) {
          bits = bits.or(BigInteger.ZERO.setBit(confirmNode.node_idx));
          //          log.debug("bits::" + bits + " node_idx::" + confirmNode.node_idx + " setBit::" + bits.setBit(confirmNode.node_idx));

          pbo.getSyncType match {
            case SyncType.ST_WALLOUT =>
              //              ArrayList[MultiTransaction.Builder]
              if (pbo.getTxDatasCount > 0) {
                dbBatchSaveList.addElement((pbo.toByteArray(), bits))
              }

              //                            !!Daos.txHelper.syncTransaction(dbsaveList, bits);

              //resend
              if (DConfig.CREATE_BLOCK_TX_CONFIRM_PERCENT > 0) {
                pbo.getTxHashList.map {
                  f => wallHashList.offer(f);
                }
              }
            //      syncTransaction.addAllTxHash(res.getTxHashList);
            //      syncTransaction.addAllTxDatas(res.getTxDatasList);
            //              DCtrl.instance.network.wallMessage("BRTDOB", Left(syncTransaction.build()), msgid)

            case _ =>
              val fromNode = DCtrl.instance.network.nodeByBcuid(pbo.getFromBcuid);
              if (fromNode != DCtrl.instance.network.noneNode) {
                bits = bits.or(BigInteger.ZERO.setBit(fromNode.node_idx));
              }
              val tmpList = new ArrayList[(Array[Byte], BigInteger)](pbo.getTxHashCount);
              pbo.getTxHashList.map { txHash =>
                tmpList.add((txHash.toByteArray(), bits))
              }
              confirmHashList.addAll(tmpList)
          }

        } else {
          log.debug("cannot find bcuid from network:" + pbo.getConfirmBcuid + "," + pbo.getFromBcuid + ",synctype=" + pbo.getSyncType);
        }

        ret.setRetCode(1)
      } catch {
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
  override def cmd: String = PCommand.BRT.name();
}
