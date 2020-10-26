package com.senko.ParDISTonSpark

import org.apache.log4j.{LogManager, PropertyConfigurator}
import org.apache.spark.graphx.{Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer
import scala.io.Source
import scala.reflect.io.File

object Main extends App {

  val log = LogManager.getLogger(Main.getClass)

  log.info("Spark Context is creating...")
  val conf = new SparkConf().setAppName("GraphDeneme").setMaster("local[*]")
  val sc = new SparkContext(conf)
  //sc.setLogLevel("ERROR")

  // create graph
  val graph: Graph[Node, Int] = GraphGenerator.createGraph(sc)

  // persist graph only memory!
  graph.persist(StorageLevel.MEMORY_ONLY)

  val (extendedComponentArray: Array[(String, Graph[Node, Int])],
       transitNetwork: Graph[VertexId, Int],
       cdm: RDD[(String, String, ListBuffer[(VertexId, VertexId, Int)])]) = PrepProcessManager.start(graph, sc)

  // after preprocessing no need to cache main graph
  graph.unpersist()

  val queryProcessor = QueryProcessor(extendedComponentArray, transitNetwork, cdm, sc)
  //queryProcessor.queryDistance(16L, 21L)
  queryProcessor.queryDistance(1L, 13L)

  //queryProcessor.queryDistance(5L, 1L)





}
