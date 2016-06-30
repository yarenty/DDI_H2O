package com.yarenty.ddi

/**
  * Created by yarenty on 30/06/2016.
  * (C)2015 SkyCorp Ltd.
  */
object Helper {


  def main(args: Array[String]) {

    val in = Array("id","timeslice","district ID","destDistrict","demand","gap",
          "traffic1","traffic2","traffic3","traffic4","weather","temp","pollution",
          "p1","p2","p3","p4","p5","p6","p7","p8","p10",
          "p11","p12","p13","p14","p15","p16","p17","p18","p19","p20",
          "p21","p22","p23","p24","p25")

//    for (i <- in) println ("val "+i+": Option[Int],")


//    var x=0
//    for (i <- in ) {
//      println("case "+x+" => " + i)
//      x += 1
//    }

    var x=0
    for (i <- in ) {
      println( "int(row("+x+")), //" + i)
      x += 1
    }

  }

}
