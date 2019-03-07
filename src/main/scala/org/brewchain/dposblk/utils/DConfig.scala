package org.brewchain.dposblk.utils

import onight.tfw.mservice.NodeHelper
import onight.tfw.outils.conf.PropHelper

object DConfig {
  val prop: PropHelper = new PropHelper(null);
  val PROP_DOMAIN = "org.bc.dpos."

  val _DBLK_EPOCH_SEC = prop.get(PROP_DOMAIN + "blk.epoch.sec", 1); //2 seconds each block
  val BLK_EPOCH_MS = prop.get(PROP_DOMAIN + "blk.epoch.ms", 500); //2 seconds each block
  val BLK_NOOP_EPOCH_MS = prop.get(PROP_DOMAIN + "blk.noop.epoch.ms", 5000); //2 seconds each block
  val BLK_MIN_EPOCH_MS = prop.get(PROP_DOMAIN + "blk.min.epoch.ms", 100); //2 seconds each block
  val BLK_WAIT_SYNC_SEC = prop.get(PROP_DOMAIN + "blk.wait.sync.sec", 10); //2 seconds each block
  
  val TXS_EPOCH_MS = prop.get(PROP_DOMAIN + "txs.epoch.ms", 500);
  val MAX_WAIT_BLK_EPOCH_MS = prop.get(PROP_DOMAIN + "max.wait.blk.epoch.ms", 10 * 1000); //1 min to wait for next block mine 

  val SYNCBLK_PAGE_SIZE = prop.get(PROP_DOMAIN + "syncblk.page.size", 10);

  val VOTE_QUORUM_RATIO = prop.get(PROP_DOMAIN + "vote.quorum.ratio", 60); //60%

  val SYNCBLK_MAX_RUNNER = prop.get(PROP_DOMAIN + "syncblk.max.runner", 10);
  val SYNCBLK_WAITSEC_NEXTRUN = prop.get(PROP_DOMAIN + "syncblk.waitsec.nextrun", 1 * 1000);
  val SYNCBLK_WAITSEC_ALLRUN = prop.get(PROP_DOMAIN + "syncblk.waitsec.allrun", 600);

  val MAX_BLK_COUNT_PERTERM = prop.get(PROP_DOMAIN + "max.blk.count.perterm", 60);
  val MIN_BLK_COUNT_PERTERM = prop.get(PROP_DOMAIN + "min.blk.count.perterm", 30);

  val MAX_VOTE_WAIT_SEC = prop.get(PROP_DOMAIN + "max.vote.wait.sec", 10);

  val TICK_BLKCTRL_SEC = prop.get(PROP_DOMAIN + "tick.blkctrl.sec", 1);
  val INITDELAY_BLKCTRL_SEC = prop.get(PROP_DOMAIN + "initdelay.blkctrl.sec", 1);

  val DTV_BEFORE_BLK = prop.get(PROP_DOMAIN + "dtv.before.blk", 5);

  val DTV_TIMEOUT_SEC = prop.get(PROP_DOMAIN + "dtv.timeout.sec", 60);

  val DTV_MUL_BLOCKS_EACH_TERM = prop.get(PROP_DOMAIN + "dtv.mul.blocks.each.term", 12);
  val DTV_BLOCKS_EACH_MINER = prop.get(PROP_DOMAIN + "dtv.blocks.each.miner", 6);

  val DTV_MAX_SUPER_MINER = prop.get(PROP_DOMAIN + "dtv.max.super.miner", 19);
  val DTV_MIN_SUPER_MINER = prop.get(PROP_DOMAIN + "dtv.min.super.miner", 5);
  val DTV_TIME_MS_EACH_BLOCK = prop.get(PROP_DOMAIN + "dtv.time.ms.each_block", 100);

  val TICK_DCTRL_MS = prop.get(PROP_DOMAIN + "tick.dctrl.ms", BLK_EPOCH_MS);
  val TICK_DCTRL_MS_TX = prop.get(PROP_DOMAIN + "tick.dctrl.ms.tx", TXS_EPOCH_MS);

  val INITDELAY_DCTRL_SEC = prop.get(PROP_DOMAIN + "initdelay.dctrl.sec", 1);

  val BLOCK_DISTANCE_COMINE = prop.get(PROP_DOMAIN + "block.distance.comine", 5);

  val BAN_MINSEC_FOR_VOTE_REJECT = prop.get(PROP_DOMAIN + "ban.minsec.for.vote.reject", 10);
  val BAN_MAXSEC_FOR_VOTE_REJECT = prop.get(PROP_DOMAIN + "ban.maxsec.for.vote.reject", 240);

  val MAX_TIMEOUTSEC_FOR_REVOTE = prop.get(PROP_DOMAIN + "max.timeoutsec.for.revote", 30);

  val MAX_TNX_EACH_BLOCK = prop.get(PROP_DOMAIN + "max.tnx.each.block", 100);
  val MIN_TNX_EACH_BLOCK = prop.get(PROP_DOMAIN + "min.tnx.each.block", 1000);
   
  val ADJUST_BLOCK_TX_MAX_TIMEMS = prop.get(PROP_DOMAIN + "adjust.block.tx.max.timems", 15000);
  val ADJUST_BLOCK_TX_MIN_TIMEMS = prop.get(PROP_DOMAIN + "adjust.block.tx.min.timems", 3000);
  val ADJUST_BLOCK_TX_STEP = prop.get(PROP_DOMAIN + "adjust.block.tx.step", 100);
  val ADJUST_BLOCK_TX_CHECKTIMES = prop.get(PROP_DOMAIN + "adjust.block.tx.checktimes", 3);
  val MAX_TNX_EACH_BROADCAST = prop.get(PROP_DOMAIN + "max.tnx.each.broadcast", 100);
  val MIN_TNX_EACH_BROADCAST = prop.get(PROP_DOMAIN + "min.tnx.each.broadcast", 100);

  val MAX_CWS_GUARANTY = prop.get(PROP_DOMAIN + "max.cws.guaranty", 10);

  //高度相同后需要等待多少个term以上才能变成cominer
  val COMINER_WAIT_BLOCKS_TODUTY = prop.get(PROP_DOMAIN + "cominer.wait.blocks.toduty", 60);

  val MAX_POSSIBLE_TERMID = prop.get(PROP_DOMAIN + "max.possible.termid", 100);

  val HEATBEAT_TICK_SEC = prop.get(PROP_DOMAIN + "heatbeat.tick.sec", 60);

  val FORCE_RESET_VOTE_TERM = prop.get(PROP_DOMAIN + "force.reset.vote.term", 0);

  val MAX_SYNC_BLOCKS = prop.get(PROP_DOMAIN + "max.sync.blocks", 3000);

  val VOTE_MAX_TERM_DISTANCE = prop.get(PROP_DOMAIN + "vote.max.term.distance", 1);

  val SYNC_SAFE_BLOCK_COUNT = prop.get(PROP_DOMAIN + "sync.safe.block.count", 8);
  
  
  val SYNC_TX_TPS_LIMIT = prop.get(PROP_DOMAIN + "sync.tx.tps.limit", 50000);//每秒钟最多1万笔交易同步

  val WAIT_BLOCK_MIN_TXN = prop.get(PROP_DOMAIN + "wait.block.min.txn", 100);//至少100笔以上就不等了
  val WAIT_BLOCK_MAX_TXN = prop.get(PROP_DOMAIN + "wait.block.max.txn", 5000);//超过5000笔以上就不等了
  
  
  val PARALL_SYNC_TX_BATCHBS = prop.get(PROP_DOMAIN + "parall.sync.tx.batchbs",Runtime.getRuntime.availableProcessors());//并行处理，交易体同步
  val PARALL_SYNC_TX_CONFIRM = prop.get(PROP_DOMAIN + "parall.sync.tx.confirm",Runtime.getRuntime.availableProcessors());//并行处理，交易确认同步
  val PARALL_SYNC_TX_WALLOUT = prop.get(PROP_DOMAIN + "parall.sync.tx.wallout",Runtime.getRuntime.availableProcessors());//并行处理，交易后确认同步

  val RUN_COMINER = prop.get(PROP_DOMAIN + "run.cominer", 1);//是否参与挖矿
    
  val CREATE_BLOCK_TX_CONFIRM_PERCENT = prop.get(PROP_DOMAIN + "create.block.tx.confirm.percent", 80); //80%

  val TX_MAX_CACHE_SIZE = prop.get(PROP_DOMAIN + "tx.max.cache.size", 300000); //80%

}

