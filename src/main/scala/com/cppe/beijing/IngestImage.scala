package com.cppe.beijing

import geotrellis.layer.{FloatingLayoutScheme, LayoutDefinition, LayoutScheme, SpatialKey}
import geotrellis.spark._
import geotrellis.spark.pipeline._
import geotrellis.spark.pipeline.ast._
import geotrellis.spark.pipeline.json._
import geotrellis.spark.pipeline.json.read._
import geotrellis.spark.pipeline.json.transform._
import geotrellis.spark.pipeline.json.write.JsonWrite
import org.apache.spark.{SparkConf, SparkContext}

object IngestImage {


  //输入输出路径设置，因为使用本地环境测试，所以这里使用的是绝对路径，如果是hdfs获取他路径，需修改设置

  val inputPath = "C:/Users/cpy/Pictures/影像下载_2112161452(1)(1).tif"

  val outputPath = "C:/Users/cpy/Pictures/output"


  def main(args: Array[String]): Unit = {

    // 创建spark conf 并配置 Kryo serializer.

    val conf =
      new SparkConf()
        .setMaster("local[*]")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.kryo.registrator", "geotrellis.spark.io.kryo.KryoRegistrator")

    implicit val sc: SparkContext = geotrellis.spark.util.SparkUtils.createSparkContext("etl pipeline", conf)
    val maskJson =
      """
        [
        |
        |  {
        |    "uri" : "file:///C:/Users/cpy/Documents/中缅dem/中缅合并.tif",
        |    "type" : "singleband.spatial.read.hadoop"
        |  },
        |
        |  {
        |    "resample_method" : "nearest-neighbor",
        |    "type" : "singleband.spatial.transform.tile-to-layout"
        |  },
        |
        |  {
        |    "crs" : "EPSG:3857",
        |    "scheme" : {
        |      "crs" : "epsg:3857",
        |      "tileSize" : 256,
        |      "resolutionThreshold" : 0.1
        |    },
        |    "resample_method" : "nearest-neighbor",
        |    "type" : "singleband.spatial.transform.buffered-reproject"
        |  },
        |
        |  {
        |    "end_zoom" : 0,
        |    "resample_method" : "nearest-neighbor",
        |    "type" : "singleband.spatial.transform.pyramid"
        |  },
        |
        |  {
        |    "name" : "mask",
        |    "uri" : "file:///C:/Users/cpy/Pictures/test/geotrellis-test/zm/",
        |    "key_index_method" : {
        |      "type" : "zorder"
        |    },
        |    "scheme" : {
        |      "crs" : "epsg:3857",
        |      "tileSize" : 256,
        |      "resolutionThreshold" : 0.1
        |    },
        |    "type" : "singleband.spatial.write"
        |  }
        |]
  """.stripMargin
    // parse the JSON above
    val list: Option[Node[Stream[(Int, TileLayerRDD[SpatialKey])]]] = maskJson.node

    list match {
       case None => println("Couldn't parse the JSON")
       case Some(node) => {
         // eval evaluates the pipeline
         // the result type of evaluation in this case would ben Stream[(Int, TileLayerRDD[SpatialKey])]
         node.eval.foreach { case (zoom, rdd) =>
           println(s"ZOOM: ${zoom}")
           println(s"COUNT: ${rdd.count}")
         }
       }
     }


  }

  def m1(): Unit = {
    /** 定义导入文件的URI与加载驱动。
     * 数据分块模式（tile-to-layout）。
     * 数据转换与冲投影等操作。
     * 数据写入地址（Lindorm）。 */
    val scheme = Left[LayoutScheme, LayoutDefinition](FloatingLayoutScheme(512))
    val jsonRead = JsonRead("file:///C:/Users/cpy/Pictures/影像下载_2112161452(1)(1).tif", `type` = ReadTypes.MultibandSpatialHadoopType)
    val jsonTileToLayout = TileToLayout(`type` = TransformTypes.SpatialTileToLayoutType)
    val jsonReproject = Reproject("EPSG:3857", scheme, `type` = TransformTypes.SpatialBufferedReprojectType)
    val jsonPyramid = Pyramid(`type` = TransformTypes.SpatialPyramidType)
    val jsonWrite = JsonWrite("mask", "file:///C:/Users/cpy/Pictures/test/geotrellis-test/pipeline/", PipelineKeyIndexMethod("zorder"), scheme, `type` = WriteTypes.SpatialType)

    val list: List[PipelineExpr] = jsonRead ~ jsonTileToLayout ~ jsonReproject ~ jsonPyramid ~ jsonWrite

    // typed way, as in the JSON example above
    val typedAst: Node[Stream[(Int, TileLayerRDD[SpatialKey])]] =
      list.node[Stream[(Int, TileLayerRDD[SpatialKey])]]
    //val result: Stream[(Int, TileLayerRDD[SpatialKey])] = typedAst.eval(sc)
  }

  def m2(): Unit = {
    val maskJson =
      """
        [
        |
        |  {
        |    "uri" : "file:///C:/Users/cpy/Pictures/影像下载_2112161452(1)(1).tif",
        |    "type" : "multiband.spatial.read.hadoop"
        |  },
        |
        |  {
        |    "resample_method" : "nearest-neighbor",
        |    "type" : "multiband.spatial.transform.tile-to-layout"
        |  },
        |
        |  {
        |    "crs" : "EPSG:3857",
        |    "scheme" : {
        |      "crs" : "epsg:3857",
        |      "tileSize" : 256,
        |      "resolutionThreshold" : 0.1
        |    },
        |    "resample_method" : "nearest-neighbor",
        |    "type" : "multiband.spatial.transform.buffered-reproject"
        |  },
        |
        |  {
        |    "end_zoom" : 0,
        |    "resample_method" : "nearest-neighbor",
        |    "type" : "multiband.spatial.transform.pyramid"
        |  },
        |
        |  {
        |    "name" : "mask",
        |    "uri" : "cassandra://192.168.2.13/geotrellis?attributes=attributes&layers=mask",
        |    "key_index_method" : {
        |      "type" : "zorder"
        |    },
        |    "scheme" : {
        |      "crs" : "epsg:3857",
        |      "tileSize" : 256,
        |      "resolutionThreshold" : 0.1
        |    },
        |    "type" : "multiband.spatial.write"
        |  }
        |]
  """.stripMargin
    // parse the JSON above
    val list: Option[Node[Stream[(Int, MultibandTileLayerRDD[SpatialKey])]]] = maskJson.node

   /* list match {
      case None => println("Couldn't parse the JSON")
      case Some(node) => {
        // eval evaluates the pipeline
        // the result type of evaluation in this case would ben Stream[(Int, TileLayerRDD[SpatialKey])]
        node.eval(sc).foreach { case (zoom, rdd) =>
          println(s"ZOOM: ${zoom}")
          println(s"COUNT: ${rdd.count}")
        }
      }
    }*/
  }
  }
