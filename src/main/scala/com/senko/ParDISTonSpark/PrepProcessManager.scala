package com.senko.ParDISTonSpark

import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph

object PrepProcessManager {

  def start(g: Graph[Node, Int], sc: SparkContext) = {
    val pre = PreProcessor(g, 1, sc)
    pre.prepareExtendedComponents()

  }

}
