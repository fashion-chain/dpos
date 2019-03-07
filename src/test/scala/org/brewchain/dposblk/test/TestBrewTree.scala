package org.brewchain.dposblk.test

import org.brewchain.ecrypto.impl.EncInstance
import java.math.BigInteger
import scala.BigInt
import scala.math.BigInt.javaBigInteger2bigInt

object TestBrewTree {

  private val AlphbetMap = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
  private val radix = AlphbetMap.length;
  private val modx = new BigInteger("" + radix);

  def hexToMapping(hexEnc: String): String = {
    var v = new BigInteger(hexEnc, 16);
    val sb = new StringBuffer();

    while (v.bitCount > 0) {
      //      println("v="+v.mod(modx))
      sb.append(AlphbetMap.charAt(v.mod(modx).intValue()));
      v = v.divide(modx);
    }
    sb.reverse().toString();
  }
  def hexToMapping(hexEnc: Array[Byte]): String = {
    var v = new BigInteger(hexEnc);
    val sb = new StringBuffer();

    while (v.bitCount > 0) {
      //      println("v="+v.mod(modx))
      sb.append(AlphbetMap.charAt(v.mod(modx).intValue()));
      v = v.divide(modx);
    }
    sb.reverse().toString();
  }

  def mapToHex(str: String): BigInt = {
    var v = BigInt(0);
    str.map { ch =>
      v = v % modx;
    }
    v;
  }
  def main(args: Array[String]): Unit = {
    val encAPI = new EncInstance();
    encAPI.startup();
    val kp = encAPI.genKeys();
    println(kp)
    println(AlphbetMap.length());
    val mAddr = hexToMapping(kp.getPrikey);
    println(kp.getAddress + "[" + kp.getPrikey.length() + "]" + "==>" + mAddr + "[" + mAddr.length() + "]");
  }
}