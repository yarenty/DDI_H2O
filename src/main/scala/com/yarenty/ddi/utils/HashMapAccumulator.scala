package com.yarenty.ddi.utils

import org.apache.spark.AccumulatorParam

import scala.collection.mutable.HashMap

/**
  * Created by yarenty on 26/06/2016.
  * (C)2015 SkyCorp Ltd.
  */
class HashMapAccumulator extends AccumulatorParam[HashMap[Int, Int]] {

  def zero(initialValue: HashMap[Int, Int]): HashMap[Int, Int] = {
    HashMap.empty(initialValue.size)
  }

  def addInPlace(h1: HashMap[Int, Int], h2: HashMap[Int, Int]): HashMap[Int, Int] = {
    println(s"adding: ${h2.keySet.head}")
    if (h1.contains(h2.keySet.head)) {
      val n = h1.get(h2.keySet.head).get + 1
      h1.update(h2.keySet.head, n)
    } else {
      h1 += h2.iterator.next
    }
    h1
  }

  def addAccumulator(h1: HashMap[Int, Int], e: Int): HashMap[Int, Int] = {

    println(s"adding: ${e}")
    if (h1.contains(e)) {
      val n = h1.get(e).get + 1
      h1.update(e, n)
    } else {
      h1 += (e -> 1)
    }
    h1
  }

}


object HashMapAccumulator extends HashMapAccumulator
