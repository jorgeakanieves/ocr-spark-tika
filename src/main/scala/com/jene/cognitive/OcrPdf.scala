package com.jene.cognitive

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.input.PortableDataStream
import org.apache.tika.metadata._
import org.apache.tika.parser._
import org.apache.tika.sax.WriteOutContentHandler
import java.io._
import org.elasticsearch.spark._
import org.elasticsearch.spark.rdd.EsSpark

/**
  * Created by jene on 02/05/2017.
  */
object OcrPdf extends Serializable{

  def extractOcr (a: (String, PortableDataStream)) = {

    val file : File = new File(a._1.drop(5))
    val myparser : AutoDetectParser = new AutoDetectParser()
    val stream : InputStream = new FileInputStream(file)
    val handler : WriteOutContentHandler = new WriteOutContentHandler(-1)
    val metadata : Metadata = new Metadata()
    val context : ParseContext = new ParseContext()

    myparser.parse(stream, handler, metadata, context)

    stream.close

    //println(handler.toString())
    //println("------------------------------------------------")

    //(a._1, handler.toString)

    new Demanda(a._1, handler.toString())
  }

  def saveToElk (a: (String, PortableDataStream)) = {

    val file : File = new File(a._1.drop(5))
    val myparser : AutoDetectParser = new AutoDetectParser()
    val stream : InputStream = new FileInputStream(file)
    val handler : WriteOutContentHandler = new WriteOutContentHandler(-1)
    val metadata : Metadata = new Metadata()
    val context : ParseContext = new ParseContext()

    myparser.parse(stream, handler, metadata, context)

    stream.close

    //println(handler.toString())
    //println("------------------------------------------------")

    //(a._1, handler.toString)
    Demanda(a._1, handler.toString())
  }


  def main(args: Array[String]) {

    System.setProperty("hadoop.home.dir", "C:\\java\\tools\\hadoop_winutils");

    //val filesPath = "/home/user/documents/*"
    val filesPath = "C:\\borrador-demanda.pdf"
    //val filesPath = "C:\\derivados.xps"
    val conf = new SparkConf().setAppName("TikaFileParser")

    conf.set("es.index.auto.create", "true")
    conf.set("es.nodes", "")
    conf.set("es.index.auto.create", "true")
    conf.set("es.mapping.id", "fileName")
    conf.set("es.spark.dataframe.write.null", "true")
    conf.set("es.spark.dataframe.write.null.values.default", "true")
    conf.set("es.read.field.empty.as.null", "false")
    conf.set("es.field.read.empty.as.null", "false")

    //conf.set("spark.kryo.registrator", "MyKryoRegistrator")

    conf.setMaster("local[*]")

    val sc = new SparkContext(conf)
    val fileData = sc.binaryFiles(filesPath)

    val ocrData = fileData.map( x => extractOcr(x))
    val ocrData2 = ocrData.map( x => print(x))
    //EsSpark.saveToEs(ocrData,"juridico/demanda")



  }
}

// define a case class
case class Demanda(filename: String, content: String)
