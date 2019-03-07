package org.brewchain.dposblk.utils

import java.util.concurrent.atomic.AtomicInteger
import onight.oapi.scala.traits.OLog
import com.google.common.cache.Cache
import org.brewchain.evmapi.gens.Tx.MultiTransaction
import com.google.common.cache.CacheBuilder
import java.util.concurrent.TimeUnit

object TxCache extends OLog {

  val recentBlkTx: Cache[String, MultiTransaction] =
    CacheBuilder.newBuilder().expireAfterWrite(DConfig.MAX_WAIT_BLK_EPOCH_MS, TimeUnit.SECONDS)
      .maximumSize(DConfig.TX_MAX_CACHE_SIZE).build().asInstanceOf[Cache[String, MultiTransaction]]

  def cacheTxs(txs: java.util.List[MultiTransaction]): Unit = {
    val s = txs.size() - 1;
    for (i <- 0 to s) {
      val tx = txs.get(i);
      recentBlkTx.put(tx.getTxHash, tx);
    }
  }

  def getTx(txhash: String): MultiTransaction = {
    val ret = recentBlkTx.getIfPresent(txhash);
    if (ret != null) {
      ret
    } else {
      null
    }

  }

}