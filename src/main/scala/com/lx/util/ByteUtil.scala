package com.lx.util

object ByteUtil {

  def bytesToHexString(bts: Array[Byte]): String = {
    val sb = new StringBuffer(bts.length)
    var tmp = ""
    for (i <- 0 until bts.length) {
      tmp = Integer.toHexString(0xFF & bts(i))
      if (tmp.length < 2) {
        sb.append(0)
      }
      sb.append(tmp.toUpperCase)
    }
    sb.toString
  }

  def hexToByteArray(inHex: String): Array[Byte] = {
    var hex = inHex
    var hexlen = hex.length

    if (hexlen % 2 == 1) {
      hexlen += 1
      hex = "0" + hex
    }


    val result =new Array[Byte](hexlen / 2)
    var j = 0
    for (i <- 0 until hexlen by 2) {
      result(j) = Integer.parseInt(inHex.substring(i, i + 2), 16).toByte
      j += 1
    }
    result
  }
}
