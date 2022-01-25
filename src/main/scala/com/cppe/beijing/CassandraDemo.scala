package com.cppe.beijing


import geotrellis.layer.{KeyExtractor, LayoutLevel, SpatialKey, ZoomedLayoutScheme}
import geotrellis.proj4.WebMercator
import geotrellis.raster.{ColorRamps, DoubleCellType, MultibandTile, RasterSource, Tile}
import geotrellis.raster.ResampleMethods.Bilinear
import geotrellis.spark.pyramid.Pyramid
import geotrellis.spark.{MultibandTileLayerRDD, RasterSourceRDD, RasterSummary}
import geotrellis.spark.store.cassandra.{CassandraLayerReader, CassandraLayerWriter}
import geotrellis.spark.store.file.{FileLayerReader, FileLayerWriter}
import geotrellis.spark.store.hadoop.{HadoopLayerReader, HadoopLayerWriter, withSaveToHadoopMethods}
import geotrellis.store.{AttributeStore, LayerId, cassandra}
import geotrellis.store.cassandra.{BaseCassandraInstance, CassandraAttributeStore}
import geotrellis.store.file.FileAttributeStore
import geotrellis.store.hadoop.HadoopAttributeStore
import geotrellis.store.index.ZCurveKeyIndexMethod
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import java.net.URI

object CassandraDemo extends App{

  val conf =
    new SparkConf()
      .setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "geotrellis.spark.io.kryo.KryoRegistrator")

  implicit val sc: SparkContext = geotrellis.spark.util.SparkUtils.createSparkContext("etl pipeline", conf)
  /** Some constants for us to refer back to */

  // a name for the layer to be ingested
  val layerName = "zm"
  // the projection we'd like things to be tiled in
  val targetCRS = WebMercator
  // an interpolation method
  val interp = Bilinear
  // the scheme for generating tile layouts
  val layoutScheme = ZoomedLayoutScheme(targetCRS, tileSize = 256)
  val paths = Seq("hdfs://localhost:9000/qwe/影像下载_2112161452(1)(1).tif")



  // Here, we parallelize a list of URIs and then turn them each into RasterSources
  // Note that reprojection/celltype manipulation is something that RasterSources allow us to do directly
  val sourceRDD: RDD[RasterSource] =
  sc.parallelize(paths, paths.length)
    // RDD is invariant
    // manually upcast it to the RasterSource type
    .map(uri => RasterSource(uri).reproject(targetCRS, method = interp): RasterSource)
    .cache()

  // collect scenes metadata
  // sadly it requires a metadata reduce step
  // that is why we may benefit from the cache() operation above
  val summary = RasterSummary.fromRDD(sourceRDD)
  // levelFor gives us the zoom and LayoutDefinition which most closely matches the computed Summary's Extent and CellSize
  val LayoutLevel(zoom, layout) = summary.levelFor(layoutScheme)

  // This is the actual in (spark, distributed) memory layer
  val tileLayerRDD = RasterSourceRDD.tiledLayerRDD(sourceRDD, layout)
  println(s"contextRDD.count(): ${tileLayerRDD.count()}")
  // 2:文件系统数据存储和读取
  /*val catalogPath: String = "C:/Users/cpy/Pictures/test/geotrellis-test/zm"
  val flieStore = FileAttributeStore(catalogPath)
  //val reader = FileLayerReader(flieStore)
  val writer = FileLayerWriter(flieStore)*/

  // 3:HDFS数据存储和读取
  /*
   val rootPath: Path = ""
  val config: Configuration
  val store1: AttributeStore = HadoopAttributeStore(rootPath, config)
  val reader = HadoopLayerReader(store1)
  val writer = HadoopLayerWriter(rootPath, store1)*/
  // 4:Cassandra数据存储和读取
 /* val instance= BaseCassandraInstance(Seq("127.0.0.1"))
  val keyspace: String = "geotrellis"
  val attrTable: String = "attributes"
  val dataTable: String ="dtqwe"
  val store = CassandraAttributeStore(instance, keyspace, attrTable)
  val reader = CassandraLayerReader(store) /* Needs the implicit SparkContext */
  val writer = CassandraLayerWriter(instance, keyspace, dataTable)*/

  // Actually write out the RDD constructed and transformed above
 //writer.write(LayerId(layerName, zoom), tileLayerRDD, ZCurveKeyIndexMethod)
 /* Pyramid.fromLayerRDD(tileLayerRDD, Some(zoom), Some(0)).write(layerName,
    writer,
    ZCurveKeyIndexMethod)*/
 val pyramid =
 Pyramid.fromLayerRDD(tileLayerRDD, Some(zoom), None)
  pyramid.levels.foreach {
    // Write each zoom level sequentially
    case (zoom, tileRdd) => {
      val keyToPath = (k: SpatialKey) => s"hdfs://localhost:9000/png/${zoom}/${k.col}/${k.row}.png"
      val getBytes = (k: SpatialKey,tile:MultibandTile) => tile.renderPng().bytes
      tileRdd.saveToHadoop(keyToPath)(getBytes)
    }}


  sc.stop()
  println("end")
}
