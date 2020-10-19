package com.senko.ParDISTonSpark

import org.apache.log4j.LogManager
import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Graph, VertexId}

import scala.collection.mutable.ListBuffer

object PrepProcessManager {
  val log = LogManager.getLogger(PrepProcessManager.getClass)

  def start(g: Graph[Node, Int], sc: SparkContext) = {
    val pre = PreProcessor(g, 1, sc)
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
      println("----------")
    }

  }

}
