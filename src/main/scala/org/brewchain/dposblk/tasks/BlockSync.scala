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
import org.brewchain.dposblk.action.PDCoinbase

object BlockSync extends LogHelper {

  val runCounter = new AtomicLong(0);
  val maxReqHeight = new AtomicLong(0);
  val running = new AtomicBoolean(false);
  def tryBackgroundSyncLogs(block_max_wanted: Int, fastNodeID: String, checkPending: Boolean)(implicit network: Network): Unit = {
    if (checkPending && PDCoinbase.isPendingBlock(block_max_wanted)) {
      log.debug("block wanted is pending " + block_max_wanted);
    } else {
      var runable = new Runnable() {
        def run() {
          BlockSync.trySyncBlock(block_max_wanted, fastNodeID);
        }
      }
      if (running.get() && block_max_wanted == maxReqHeight.get) {
        log.debug("waiting...block:" + block_max_wanted + ",execpool=" + Scheduler.scheduler.getActiveCount
          + ",running=" + running.get);
      } else {
        Scheduler.runManager(runable);
      }
    }
    //Scheduler.runManager(runable,0,100, TimeUnit.MILLISECONDS)
  }
  def trySyncBlock(block_max_maybe_wanted: Int, fastNodeID: String)(implicit network: Network): Unit = {
    //    log.debug("syncblocklog --> block_max_maybe_wanted:" + block_max_maybe_wanted);
    Thread.currentThread().setName("block-sync:" + block_max_maybe_wanted);
    if (network.nodeByBcuid(fastNodeID) == network.noneNode) {
      log.error("cannot sync log bcuid not found in dposnet:nodeid=" + fastNodeID + ":");
      running.set(false)
    } else {
      val cn = DCtrl.instance.cur_dnode;
      //
      //      log.debug("try sync block: Max block= " + block_max_wanted + ",cur=" + cn.getCurBlock + ",running=" + running.get)
      try {
        //        log.debug("syncblocklog --> try sync block: want max block= " + block_max_maybe_wanted + ",maxreqheight=" + maxReqHeight.get + ",cur=" + cn.getCurBlock + ",running=" + running.get)
        var skipreq = false;
        maxReqHeight.synchronized({
          if (maxReqHeight.get > block_max_maybe_wanted && cn.getCurBlock >= block_max_maybe_wanted) {
            log.debug("not need to sync block: Max block=" + block_max_maybe_wanted + ",maxreqheight=" + maxReqHeight.get + ",cur=" + cn.getCurBlock + ",running=" + running.get)
            skipreq = true;
            return ;
          } else {
            maxReqHeight.set(block_max_maybe_wanted)
          }
        })
        var lastLogTime = 0L;

        while (!running.compareAndSet(false, true) && !skipreq) {
          try {
            if (System.currentTimeMillis() - lastLogTime > 1 * 1000) {
              log.error("waiting for runnerSyncBatch:curheight=" + cn.getCurBlock + ",runCounter=" + runCounter.get + ",wantblock= " + block_max_maybe_wanted + ",maxreqheight=" + maxReqHeight.get)
              lastLogTime = System.currentTimeMillis()
            }
            if (maxReqHeight.get > block_max_maybe_wanted || cn.getCurBlock > block_max_maybe_wanted) {
              log.error("not need to sync block.: Max block=" + block_max_maybe_wanted + ",maxreqheight=" + maxReqHeight.get + ",cur=" + cn.getCurBlock + ",running=" + running.get)
              skipreq = true;
            } else {
              this.synchronized(this.wait(DConfig.SYNCBLK_WAITSEC_NEXTRUN))
            }
          } catch {
            case t: InterruptedException =>
            case e: Throwable =>
          }
        }

        if (maxReqHeight.get > block_max_maybe_wanted) {
          log.error("not need to sync block.: Max block=" + block_max_maybe_wanted + ",maxreqheight=" + maxReqHeight.get + ",cur=" + cn.getCurBlock + ",running=" + running.get)
          skipreq = true;
        }

        if (!skipreq) {
          val block_max_wanted = Math.min(cn.getCurBlock + DConfig.MAX_SYNC_BLOCKS, block_max_maybe_wanted);
          val pagecount =
            ((block_max_wanted - cn.getCurBlock) / DConfig.SYNCBLK_PAGE_SIZE).asInstanceOf[Int]
          +(if ((block_max_wanted - cn.getCurBlock) % DConfig.SYNCBLK_PAGE_SIZE == 0) 1 else 0)

          var cc = cn.getCurBlock + 1;
          var runcounter = 0;
          while (cc <= block_max_wanted) {
            //            log.debug("syncblocklog --> DTask_SyncBlock cc:"+ cc + " endidx:" + Math.min(cc + DConfig.SYNCBLK_PAGE_SIZE - 1, block_max_wanted))
            val runner = DTask_SyncBlock(startIdx = cc, endIdx =
              Math.min(cc + DConfig.SYNCBLK_PAGE_SIZE - 1, block_max_wanted),
              network = network, fastNodeID, runCounter)
            cc += DConfig.SYNCBLK_PAGE_SIZE
            var runed = false;
            runCounter.synchronized({
              if (runCounter.get < DConfig.SYNCBLK_MAX_RUNNER) {
                runCounter.incrementAndGet();
                Scheduler.runOnce(runner);
                runcounter = runcounter + 1;
                runed = true;
                //                log.debug("syncblocklog --> sync task runned")
              }
            })
            var lastLogTime = 0L;
            //            log.debug("syncblocklog --> runCounter.get:" + runCounter.get + " DConfig.SYNCBLK_MAX_RUNNER:" + DConfig.SYNCBLK_MAX_RUNNER)
            var ccwait = 0;
            while (runCounter.get >= DConfig.SYNCBLK_MAX_RUNNER && ccwait < 30) {
              //wait... for next runner
              try {
                if (System.currentTimeMillis() - lastLogTime > 1 * 1000) {
                  log.error("syncblocklog --> waiting for runner:cur=" + runCounter.get + " MAX_RUNNER=" + DConfig.SYNCBLK_MAX_RUNNER + ",cc=" + ccwait)
                  lastLogTime = System.currentTimeMillis()
                }
                ccwait = ccwait + 1
                Thread.sleep(DConfig.SYNCBLK_WAITSEC_NEXTRUN)
              } catch {
                case t: InterruptedException =>
                case e: Throwable =>
              }
            }
            //            log.debug("syncblocklog --> runCounter.get:" + runCounter.get)
            if (!runed) {
              log.error("force to run::syncblocklog --> rerun sync task:" + runCounter.get + ",execpool=" + Scheduler.scheduler.getActiveCount + " MAX_RUNNER=" + DConfig.SYNCBLK_MAX_RUNNER + ",cc=" + ccwait)
              runCounter.synchronized {
                runCounter.incrementAndGet();
                runner.runOnce()
                runcounter = runcounter + 1;
                runed = true;
              }
            }
          }
          var ccwait = 0;
          while (runcounter > 0 && runCounter.get > 0 && ccwait < 20) {
            if (System.currentTimeMillis() - lastLogTime > 10 * 1000) {
              log.error("waiting for log syncs end:" + runCounter.get + ",cc=" + ccwait + ",counter=" + runcounter + ",execpool=" + Scheduler.scheduler.getActiveCount
                + ",blockwanted=" + block_max_maybe_wanted);
              lastLogTime = System.currentTimeMillis()
            }
            ccwait = ccwait + 1;
            Thread.sleep(DConfig.SYNCBLK_WAITSEC_NEXTRUN * 1000)
          }
          log.error("finished sync:" + runCounter.get + ",cc=" + ccwait + ",runcount=" + runcounter + ",blockwanted=" + block_max_maybe_wanted
            + ",curblock=" + cn.getCurBlock + ",execpool=" + Scheduler.scheduler.getActiveCount);

          //          log.debug("syncblocklog --> finished init follow up logs:" + DCtrl.curDN().getCurBlock);
        } else {
          log.error("skip request follow up logs:" + DCtrl.curDN().getCurBlock + ",block_wanted=" + block_max_maybe_wanted
            + ",from=" + fastNodeID + ",execpool=" + Scheduler.scheduler.getActiveCount);
        }
      } catch {
        case t: Throwable =>
          log.error("get error in blocksync:" + ",blockwanted=" + block_max_maybe_wanted
            + ",curblock=" + cn.getCurBlock + ",execpool=" + Scheduler.scheduler.getActiveCount, t);
      } finally {
        running.set(false)
        Thread.currentThread().setName("dpos-schedulor");

      }
    }
    //
  }
}