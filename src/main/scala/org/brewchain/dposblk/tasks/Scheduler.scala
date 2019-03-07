package org.brewchain.dposblk.tasks

import onight.oapi.scala.traits.OLog
import java.util.concurrent.ScheduledThreadPoolExecutor
import java.util.HashMap
import java.util.concurrent.TimeUnit

import java.util.concurrent.ScheduledFuture

object Scheduler extends OLog {
  val scheduler = new ScheduledThreadPoolExecutor(Runtime.getRuntime.availableProcessors()*2);
  val schedulerForTx = new ScheduledThreadPoolExecutor(Runtime.getRuntime.availableProcessors());
  val schedulerForDCtrl = new ScheduledThreadPoolExecutor(3);

  val schedulerManager = new ScheduledThreadPoolExecutor(Runtime.getRuntime.availableProcessors());
  def shutdown() {
    scheduler.shutdown()
    schedulerForTx.shutdown()
  }

  val runnerByGroupAddr = new HashMap[String, HashMap[String, ScheduledFuture[_]]]();

  def scheduleWithFixedDelay(command: Runnable,
    initialDelay: Long,
    delay: Long,
    unit: TimeUnit): ScheduledFuture[_] = {
    scheduler.scheduleWithFixedDelay(command, initialDelay, delay, unit);
  }
  
  def scheduleWithFixedDelayTx(command: Runnable,
    initialDelay: Long,
    delay: Long,
    unit: TimeUnit): ScheduledFuture[_] = {
    schedulerForTx.scheduleWithFixedDelay(command, initialDelay, delay, unit);
  }

  def stopGroupRunners(group: String) {
    log.debug("stopGroupRunners:" + group);
    runnerByGroupAddr.synchronized({
      val group_runners = runnerByGroupAddr.get(group)
      if (group_runners != null) {
        val it = group_runners.values().iterator();
        while (it.hasNext()) {
          it.next().cancel(true);
        }
        runnerByGroupAddr.remove(group);
      }
    })
  }
  def runOnce(runner: Runnable): Unit = {
    scheduler.submit(runner);
  }
//  def runManager(command: Runnable,
//    initialDelay: Long,
//    delay: Long,
//    unit: TimeUnit): Unit = {
//    schedulerManager.scheduleWithFixedDelay(command, initialDelay, delay, unit)
//  }
  def runManager(runner: Runnable): Unit = {
    schedulerManager.submit(runner);
  }
//
//  def updateRunner(group: String, addr: String, runner: Runnable, delay: Long) = {
//    runnerByGroupAddr.synchronized({
//      var group_runners = runnerByGroupAddr.get(group)
//      if (group_runners == null) {
//        group_runners = new HashMap[String, ScheduledFuture[_]]();
//        runnerByGroupAddr.put(group, group_runners)
//      }
//      val sf = group_runners.get(addr);
//      if (sf != null) {
//        sf.cancel(true);
//      }
//      if (delay > 0) {
//        val sf1 = Scheduler.scheduleWithFixedDelay(runner, 0, delay, TimeUnit.SECONDS)
//        group_runners.put(addr, sf1);
//      } else {
//        log.debug("Stop scheduler:" + sf);
//      }
//
//    })
//  }
}