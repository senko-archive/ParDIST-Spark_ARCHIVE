package com.senko.ParDISTonSpark

import org.apache.spark.graphx.{Graph, VertexId}

import scala.util.control.Breaks.{break, breakable}

object ShortestPath {

  def singleSourceSingleTargetDijkstra[VD](g: Graph[VD, Int], origin: VertexId, destination: VertexId) = {
    var g2 = g.mapVertices(
      (vid, vd) => (false, if (vid == origin) 0 else Int.MaxValue, List[VertexId]())
    )

    breakable {
      for(i <- 1L to g.vertices.count -1) {

        val currentVertexId = g2.vertices.filter(!_._2._1)
          .fold((0L, (false, Int.MaxValue, List[VertexId]())))((a,b) =>
            if (a._2._2 < b._2._2) a else b)
          ._1

        if(currentVertexId == destination)
          break()

        val newDistances = g2.aggregateMessages[(Int, List[VertexId])](
          ctx => if (ctx.srcId == currentVertexId)
            ctx.sendToDst((ctx.srcAttr._2 + ctx.attr, ctx.srcAttr._3 :+ ctx.srcId)),
          (a, b) => (if (a._1 < b._1) a else b)
        )
        if(newDistances.count() != 0) {
          g2 = g2.outerJoinVertices(newDistances)((vid, vd, newSum) => {
            val newSumVal: (Int, List[VertexId]) = newSum.getOrElse((Int.MaxValue, List[VertexId]()))
            (vd._1 || vid == currentVertexId, math.min(vd._2, newSumVal._1),
              if(vd._2 < newSumVal._1) vd._3 else newSumVal._2)
          })
        }


      }
    }

    val result = g2.vertices.filter(item => item._1 == destination).map(item => (item._1, item._2._2, item._2._3)).collect().head
    result

  }

}
