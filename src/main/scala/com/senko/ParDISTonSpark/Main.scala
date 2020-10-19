package com.senko.ParDISTonSpark

import org.apache.log4j.{LogManager, PropertyConfigurator}
import org.apache.spark.graphx.Graph
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

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

  PrepProcessManager.start(graph, sc)






}
