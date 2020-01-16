package com.lx.util

object MapUtils {
  def isEmpty[K, V](map: Map[K, V]): Boolean = {
    map == null || map.isEmpty
  }
}
