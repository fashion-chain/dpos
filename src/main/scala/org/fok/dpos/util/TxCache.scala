package org.fok.dpos.util

import java.util.concurrent.atomic.AtomicInteger
import onight.oapi.scala.traits.OLog
import org.fok.core.model.Transaction.TransactionInfo
import java.util.concurrent.TimeUnit

import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Element;
import net.sf.ehcache.store.MemoryStoreEvictionPolicy;

object TxCache extends OLog {

  //  val recentBlkTx: Cache[Array[Byte], TransactionInfo] =
  //    CacheBuilder.newBuilder().expireAfterWrite(DConfig.MAX_WAIT_BLK_EPOCH_MS, TimeUnit.SECONDS)
  //      .maximumSize(DConfig.TX_MAX_CACHE_SIZE).build().asInstanceOf[Cache[Array[Byte], TransactionInfo]]
  val recentBlkTx: Cache = new Cache("dpos_recentblocktx", 300000, MemoryStoreEvictionPolicy.LRU,
    true, "./dpos_recentblocktx", true,
    0, 0, true, 120, null)

  def cacheTxs(txs: java.util.List[TransactionInfo]): Unit = {
    val s = txs.size() - 1;
    for (i <- 0 to s) {
      val tx = txs.get(i);
      var element = new Element(tx.getHash.toByteArray(), tx);
      recentBlkTx.put(element);
    }
  }

  def getTx(txhash: Array[Byte]): TransactionInfo = {
//    val ret = recentBlkTx.getIfPresent(txhash);
    val ret = recentBlkTx.get(txhash);
    if (ret != null) {
      ret.getValue.asInstanceOf[TransactionInfo]
    } else {
      null
    }

  }

}