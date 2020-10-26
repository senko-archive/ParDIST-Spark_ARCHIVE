package com.senko.ParDISTonSpark

import org.apache.log4j.LogManager
import org.apache.spark.graphx.{Graph, VertexId}

import scala.collection.mutable.ListBuffer

object GraphHelpers {

  var log = LogManager.getLogger(GraphHelpers.getClass)

  def getPartitions(graph: Graph[Node, Int]) = {
    graph.vertices.map(vertex => vertex._2.partition).distinct().collect()
  }

  def getPartitionNodes(graph: Graph[Node, Int], partitionName: String) = {
    graph.vertices.filter(vertex => vertex._2.partition == partitionName)
  }

  def generateBorderCouples(borderList: Array[(VertexId, String)]) = {

    val borderListBuffer = new ListBuffer[((VertexId, String), (VertexId, String))]
    for(borderNodeA <- borderList) {
      for(borderNodeB <- borderList) {
        if(borderNodeA._1 != borderNodeB._1) {
          borderListBuffer.append(((borderNodeA._1, borderNodeA._2), (borderNodeB._1, borderNodeB._2)))
        }
      }
    }
/*
    for(elem <- borderListBuffer) {
      for(i <- 0 to borderList.length -1) {
        if(elem._1._1 == borderListBuffer(i)._2._1 && elem._2._1 == borderListBuffer(i)._1._1) {
          borderListBuffer(i) = ((0L, ""), (0L, ""))
        }
      }
    }
*/
    borderListBuffer.filter(item => item._1 != item._2)
  }

  def getPartitionGraph(graph: Graph[Node, Int], partition: String) = {
    val extendedSubGraph = graph.subgraph(epred = triplet => {
      triplet.dstAttr.partition == partition && triplet.srcAttr.partition == partition
    },
      vpred = (vertexID, VD) => {
        VD.partition == partition
      })
    extendedSubGraph
  }

}
