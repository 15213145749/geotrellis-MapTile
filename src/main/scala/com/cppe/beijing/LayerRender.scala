//package com.cppe.beijing
//
//import geotrellis.raster.Tile
//import geotrellis.raster.io.geotiff.MultibandGeoTiff
//import geotrellis.raster.render.{ColorMap, ColorRamp, RGB}
//import geotrellis.spark._
//import geotrellis.spark.io._
//import geotrellis.spark.io.file._
//import geotrellis.spark.util.SparkUtils
//import org.apache.spark.SparkConf
//import org.apache.spark.rdd.RDD
//
//object LayerRender {
//  val colorMap1 =
//    ColorMap(
//      Map(
//        0 -> RGB(0,0,0),
//        1 -> RGB(255,255,255)
//      )
//    )
//  val colorRamp =
//    ColorRamp(RGB(0,0,0), RGB(255,255,255))
//      .stops(100)
//      .setAlphaGradient(0xFF, 0xAA)
//  val zoomID=6
//
//  def main(args: Array[String]): Unit = {
//    println("hello geotrellis")
//    /* Some location on your computer */
//    implicit val sc = SparkUtils.createSparkContext("GeoTrellis ETL SinglebandIngest", new SparkConf(true).setMaster("local"))
//    val catalogPath: String ="D:\\IdeaProjects\\GeotrellisETL\\data\\DL3857_output3857"
//    val store: AttributeStore = FileAttributeStore(catalogPath)
//    val reader = FileLayerReader(store,catalogPath)
//    val rdd: MultibandTileLayerRDD[SpatialKey] = ???
//
//    // Convert the values of the layer to SinglebandGeoTiffs
//    val geoTiffRDD: RDD[(SpatialKey, MultibandGeoTiff)] = rdd.toGeoTiffs()
//
//    // Convert the GeoTiffs to Array[Byte]]
//    val byteRDD: RDD[(SpatialKey, Array[Byte])] = geoTiffRDD.mapValues { _.toByteArray }
//
//    // In order to save files to S3, we need a function that converts the
//    // Keys of the layer to URIs of their associated values.
//    val keyToURI = (k: SpatialKey) => s"s3://path/to/geotiffs/${k.col}_${k.row}.tif"
//
//    byteRDD.saveAsObjectFile(keyToURI)
//  }
//
//
//
//}
