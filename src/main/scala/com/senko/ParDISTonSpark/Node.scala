package com.senko.ParDISTonSpark

import org.apache.spark.graphx.VertexId

import scala.collection.mutable.ListBuffer

case class IDT(borderIDTList: ListBuffer[(VertexId, String, Int)])
case class Node(name: String, isBorderNode: Boolean, partition: String, var idt: Option[IDT])