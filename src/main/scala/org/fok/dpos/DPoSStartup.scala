package org.fok.dpos

import onight.osgi.annotation.NActorProvider
import com.google.protobuf.Message
import onight.oapi.scala.commons.SessionModules
import org.apache.felix.ipojo.annotations.Validate
import org.apache.felix.ipojo.annotations.Invalidate
import onight.tfw.otransio.api.NonePackSender
import onight.oapi.scala.traits.OLog
import java.net.URL
import onight.tfw.mservice.NodeHelper
import java.util.concurrent.TimeUnit
import org.fok.dpos.task.DCtrl
import org.fok.dpos.task.DPosNodeController
import org.fok.dpos.util.DConfig
import org.fok.dpos.task.Scheduler
import org.fok.p22p.utils.LogHelper
import org.fok.dpos.task.TransactionSync
import org.fok.dpos.task.TxSync
import onight.tfw.outils.serialize.UUIDGenerator

@NActorProvider
class DPoSStartup extends PSMDPoSNet[Message] {

  override def getCmds: Array[String] = Array("SSS");

  @Validate
  def init() {

    //    System.setProperty("java.protocol.handler.pkgs", "org.fc.brewchain.url");
    log.debug("startup:");
    new Thread(new DPoSBGLoader()).start()

    log.debug("tasks inited....[OK]");
  }

  @Invalidate
  def destory() {
    DCtrl.instance.isStop = true;
  }

}

class DPoSBGLoader() extends Runnable with LogHelper {
  def run() = {
//    URLHelper.init();
    System.setProperty("java.protocol.handler.pkgs", "org.fc.brewchain.bcapi.url");
    while (!Daos.isDbReady() //        || MessageSender.sockSender.isInstanceOf[NonePackSender]
    ) {
      log.debug("Daos Or sockSender Not Ready..:pzp=" + Daos.pzp+",dbready="+Daos.isDbReady())
      Thread.sleep(1000);
    }

    var dposnet = Daos.pzp.networkByID("dpos")

    while (dposnet == null
      || dposnet.node_bits().bitCount <= 0 || !dposnet.inNetwork()) {
      dposnet = Daos.pzp.networkByID("dpos")
      if (dposnet != null) {
        MDCSetBCUID(dposnet)
      }
      log.debug("dposnet not ready. dposnet=" + dposnet)
      Thread.sleep(5000);
    }
    //    RSM.instance = RaftStateManager(raftnet);

    //     Daos.actdb.getNodeAccount();

    while (Daos.actdb.getNodeAccountAddress == null) {
      log.debug("dpos cws account not ready. ")
      Thread.sleep(5000);
    }
    val naccount = Daos.actdb.getNodeAccountAddress;
    Daos.actdb.startBlockChain(dposnet.root().bcuid, dposnet.root().v_address, dposnet.root().name)
    UUIDGenerator.setJVM(dposnet.root().bcuid.substring(1))
    dposnet.changeNodeVAddr(Daos.enc.bytesToHexStr(naccount));
    log.debug("dposnet.initOK:My Node=" + dposnet.root() + ",CoAddr=" + dposnet.root().v_address
        +",dctrl.tick="+Math.min(DConfig.TICK_DCTRL_MS, DConfig.BLK_EPOCH_MS)) // my node

    DCtrl.instance = DPosNodeController(dposnet);

    Scheduler.schedulerForDCtrl.scheduleWithFixedDelay(DCtrl.instance, DConfig.INITDELAY_DCTRL_SEC,
      Math.min(DConfig.TICK_DCTRL_MS, DConfig.BLK_EPOCH_MS), TimeUnit.MILLISECONDS)

    TxSync.instance = TransactionSync(dposnet);
    Scheduler.scheduleWithFixedDelayTx(TxSync.instance, DConfig.INITDELAY_DCTRL_SEC,
      Math.min(DConfig.TICK_DCTRL_MS_TX, DConfig.TXS_EPOCH_MS), TimeUnit.MILLISECONDS)

    //    Daos
    //    Scheduler.scheduleWithFixedDelay(RSM.instance, RConfig.INITDELAY_RSM_SEC,
    //      RConfig.TICK_RSM_SEC, TimeUnit.SECONDS)

  }
}