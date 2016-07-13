package com.yarenty.ddi.utils

import java.io.{File, PrintWriter, FileOutputStream}

import com.yarenty.ddi.MLProcessor
import com.yarenty.ddi.MLProcessor._
import hex.Model
import hex.tree.drf.DRFModel
import org.apache.spark.h2o
import org.apache.spark.h2o.H2OFrame
import org.apache.spark.h2o._
import org.apache.spark.sql.DataFrame
import water.{Key, AutoBuffer}
import water.fvec.{H2OFrame, Frame}

/**
  * Created by yarenty on 13/07/2016.
  * (C)2015 SkyCorp Ltd.
  */
object Utils {


  def saveJavaPOJOModel(model:DRFModel, path:String): Unit ={
    val name = path + System.currentTimeMillis() + ".java"
    val om = new FileOutputStream(name)
    model.toJava(om, false, false)
    println("Java POJO model saved as"+name)
  }

  def saveBinaryModel(model:DRFModel, path:String): Unit ={
    val name = path + System.currentTimeMillis() + ".hex"
    val omab = new FileOutputStream(name)
    val ab = new AutoBuffer(omab, true)
    model.write(ab)
    ab.close()
    println("HEX(iced) model saved as"+name)
  }


  def saveCSV(f:Frame, name: String): Unit ={
    val csv = f.toCSV(true, false)
    val csv_writer = new PrintWriter(new File(name))
    while (csv.available() > 0) {
      csv_writer.write(csv.read.toChar)
    }
    csv_writer.close
  }


 /*
  def saveOutput(smOutputTest: H2OFrame, fName: String): Unit = {

    import MLProcessor.sqlContext
    val names = Array("timeslice", "districtID", "gap", "predict")

    val key = Key.make("output").asInstanceOf[Key[Frame]]
    val out = new Frame(key, names, smOutputTest.vecs(names))
    val zz = new h2o.H2OFrame(out)
    val odf = asDataFrame(zz)

    odf.registerTempTable("out")

    val a = sqlContext.sql("select timeslice, districtID, gap, IF(predict<0.5,cast(0.0 as double), predict) as predict from out")
    a.registerTempTable("gaps")
    val o = sqlContext.sql(" select timeslice, districtID, sum(gap) as gap, sum(predict) as predict from gaps " +
      "  group by timeslice,districtID")


    o.take(20).foreach(println)
    val toSee = new H2OFrame(o)
    println(s" output should be visible now ")

    val n = fName.split("/")
    val name = n(n.length - 1)
    saveCSV(o,"/opt/data/season_1/out/final_" + name + ".csv" )
  }
  */

}
