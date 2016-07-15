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



  val districts: Map[String, Int] = {
    Map(
      "7f84bdfc2b6d4541e1f6c0a3349e0251" -> 52,
      "fff4e8465d1e12621bc361276b6217cf" -> 32,
      "58c7a4888306d8ff3a641d1c0feccbe3" -> 3,
      "74ec84f1cf75cf89ae176c8c6ceec5ba" -> 49,
      "c4ec24e0a58ebedaa1661e5c09e47bb5" -> 54,
      "91690261186ae5bee8f83808ea1e4a01" -> 20,
      "f9280c5dab6910ed44e518248048b9fe" -> 41,
      "73ff8ef735e1d68f0cdcbb84d788f2b6" -> 40,
      "4f4041f7db0c7f69892d9b74c1a7efa1" -> 10,
      "4725c39a5e5f4c188d382da3910b3f3f" -> 23,
      "62afaf3288e236b389af9cfdc5206415" -> 48,
      "1afd7afbc81ecc1b13886a569d869e8a" -> 46,
      "0a5fef95db34383403d11cb6af937309" -> 63,
      "2350be163432e42270d2670cb3c02f80" -> 18,
      "fc34648599753c9e74ab238e9a4a07ad" -> 27,
      "38d5ad2d22b61109fd8e7b43cd0e8901" -> 24,
      "90c5a34f06ac86aee0fd70e2adce7d8a" -> 1,
      "d4ec2125aff74eded207d2d915ef682f" -> 51,
      "82cc4851f9e4faa4e54309f8bb73fd7c" -> 8,
      "ca064c2682ca48c6a21de012e87c0df5" -> 42,
      "a814069db8d32f0fa6e188f41059c6e1" -> 17,
      "1c60154546102e6525f68cb4f31e0657" -> 56,
      "4b9e4cf2fbdc8281b8a1f9f12b80ce4d" -> 5,
      "1cbfbdd079ef93e74405c53fcfff8567" -> 6,
      "364bf755f9b270f0f9141d1a61de43ee" -> 21,
      "52d7b69796362a8ed1691a6cc02ddde4" -> 33,
      "307afa4120c590b3a46cf4ff5415608a" -> 16,
      "d05052b4bda7662a084f235e880f50fa" -> 36,
      "bf44d327f0232325c6d5280926d7b37d" -> 64,
      "08f5b445ec6b29deba62e6fd8b0325a6" -> 43,
      "3a43dcdff3c0b66b1acb1644ff055f9d" -> 25,
      "a735449c5c09df639c35a7d61fad3ee5" -> 62,
      "44c097b7bd219d104050abbafe51bd49" -> 35,
      "ba32abfc048219e933bee869741da911" -> 57,
      "d524868ce69cb9db10fc5af177fb9423" -> 59,
      "445ff793ebd3477d4a2e0b36b2db9271" -> 55,
      "2920ece99323b4c111d6f9affc7ea034" -> 14,
      "8bb37d24db1ad665e706c2655d9c4c72" -> 34,
      "f2c8c4bb99e6377d21de71275afd6cd2" -> 2,
      "a5609739c6b5c2719a3752327c5e33a7" -> 19,
      "de092beab9305613aca8f79d7d7224e7" -> 61,
      "08232402614a9b48895cc3d0aeb0e9f2" -> 50,
      "2407d482f0ffa22a947068f2551fe62c" -> 28,
      "1ecbb52d73c522f184a6fc53128b1ea1" -> 66,
      "929ec6c160e6f52c20a4217c7978f681" -> 7,
      "b26a240205c852804ff8758628c0a86a" -> 4,
      "52a4e8aaa12f70020e889aed8fd5ddbc" -> 29,
      "52e56004d92b8c74d53e1e42699cba6f" -> 26,
      "4f8d81b5c31af5d1ba579a65ddc8a5cb" -> 38,
      "87285a66236346350541b8815c5fae94" -> 22,
      "825c426141df01d38c1b9e9c5330bdac" -> 30,
      "49ac89aa860c27e26c0836cb8dab2df2" -> 60,
      "8316146a6f78cc6d9f113f0390859417" -> 44,
      "b702e920dcd2765e624dc1ce3a770512" -> 9,
      "74c1c25f4b283fa74a5514307b0d0278" -> 12,
      "2301bc920194c95cf0c7486e5675243c" -> 31,
      "4b7f6f4e2bf237b6cc58f57142bea5c0" -> 13,
      "d5cb17978de290c56e84c9cf97e63186" -> 15,
      "b05379ac3f9b7d99370d443cfd5dcc28" -> 37,
      "825a21aa308dea206adb49c4b77c7805" -> 65,
      "cb6041cc08444746caf6039d8b9e43cb" -> 58,
      "c9f855e3e13480aad0af64b418e810c3" -> 45,
      "693a21b16653871bbd455403da5412b4" -> 39,
      "f47f35242ed40655814bc086d7514046" -> 53,
      "dd8d3b9665536d6e05b29c2648c0e69a" -> 11,
      "3e12208dd0be281c92a6ab57d9a6fb32" -> 47
    )

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
