package com.jene.cognitive

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.FunSuite

/**
  * Created by jene on 02/05/2017.
  */
class OcrPdf$Test extends FunSuite {

  test("TikaFileParser App should recognize text from pdf files") {

    System.setProperty("hadoop.home.dir", "C:\\java\\tools\\hadoop_winutils");

    val ocrTika = OcrPdf;
    //val filesPath = "/home/user/documents/*"
    //val filesPath = "C:\\borrador-demanda.pdf"
    val filesPath = "C:\\derivados.xps"

    val conf = new SparkConf().setAppName("TikaFileParser")

    conf.setMaster("local[*]")

    val sc = new SparkContext(conf)
    val fileData = sc.binaryFiles(filesPath)
    val rddTexts = fileData.map( x => ocrTika.extractOcr(x))

    rddTexts.foreach(println)

    //rddTexts.take(1).foreach(x=> assert( x._2 != ""))


    //val rddTexts = fileData.map(x => ocrTika.tikaFunc(x))
    //val rddTexts2 = rddTexts.map(x => ocrTika.tikaFunc2(x))

    //rddTexts.foreach(println)
    //println(rddTexts.take(1).toString)
    //assert(rddTexts.first() != "" )
    //val element = rddTexts.take(1).map(x => assert(x != "" ))

  }

}
