package org.brewchain.dposblk.utils

import java.util.concurrent.atomic.AtomicInteger
import onight.oapi.scala.traits.OLog


object BlkTxCalc extends OLog{

  var blockTx = DConfig.MAX_TNX_EACH_BLOCK

  def getBestBlockTxCount(termMaxBlk: Int): Int = {
    return blockTx;
  }
  
  val needChangeCounter = new AtomicInteger(0);

  def adjustTx(costMS: Long) = {
    if (costMS > DConfig.ADJUST_BLOCK_TX_MAX_TIMEMS) {
      if (blockTx > DConfig.MIN_TNX_EACH_BLOCK &&
          needChangeCounter.incrementAndGet()>DConfig.ADJUST_BLOCK_TX_CHECKTIMES) {
        blockTx = blockTx - DConfig.ADJUST_BLOCK_TX_STEP;
        blockTx = Math.max(blockTx, DConfig.MIN_TNX_EACH_BLOCK);
        log.error("slow down -- decrease tx count for each block to :"+blockTx);
        needChangeCounter.set(0)
      }
    } else if (costMS < DConfig.ADJUST_BLOCK_TX_MIN_TIMEMS) {
      if (blockTx < DConfig.MAX_TNX_EACH_BLOCK &&
          needChangeCounter.incrementAndGet()>DConfig.ADJUST_BLOCK_TX_CHECKTIMES) {
        blockTx = blockTx + DConfig.ADJUST_BLOCK_TX_STEP;
        log.error("speed up increase tx count for each block to :"+blockTx);
        needChangeCounter.set(0)
      }
    }
  }

}