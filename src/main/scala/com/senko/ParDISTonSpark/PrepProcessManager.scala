package com.senko.ParDISTonSpark

import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Graph, VertexId}
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer

object PrepProcessManager {
  val log: Logger = LogManager.getLogger(PrepProcessManager.getClass)

  def start(g: Graph[Node, Int], sc: SparkContext): (Array[(String, Graph[Node, Int])], Graph[VertexId, Int], RDD[(String, String, ListBuffer[(VertexId, VertexId, Int)])]) = {
    val pre = PreProcessor(g, 4, sc) // default 4 parallel partition work at same time
    val (extendedComponentArray, transitNetwork) = pre.prepareExtendedComponents()
    val CDM: ListBuffer[(String, String, ListBuffer[(VertexId, VertexId, Int)])] = pre.prepareComponentDistanceMatrix()

    log.info("CMD is printing....")
    for(elem <- CDM) {
      println(elem)
    }

    log.info("Extended Components are printing ...")
    for(ex <- extendedComponentArray) {
      println("----------")
      println(ex._1)
      ex._2.vertices.collect().foreach(println(_))
      println(".......")
      ex._2.edges.collect().foreach(println(_))
      println("----------")
    }

    // make CDM spark RDD
    val cdmRDD = sc.parallelize(CDM)

    // return list of extendedComponents, tranistNetwork and CDM
    (extendedComponentArray, transitNetwork, cdmRDD)

  }

}
