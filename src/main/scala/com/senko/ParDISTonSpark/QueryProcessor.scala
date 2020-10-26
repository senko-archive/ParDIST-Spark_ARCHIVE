package com.senko.ParDISTonSpark

import java.util.concurrent.Executors

import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Graph, VertexId}
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

class QueryProcessor(extendedNetworkList: Array[(String, Graph[Node, Int])],
                     transitNetwork:  Graph[VertexId, Int],
                     CDM: RDD[(String, String, ListBuffer[(VertexId, VertexId, Int)])],
                     sc: SparkContext,
                     ec: ExecutionContext) {
  var log: Logger = LogManager.getLogger(this.getClass)
  implicit val ec_inner: ExecutionContext = ec

  def queryDistance(source: VertexId, destination: VertexId): Unit = {

    // find source partition.
    val sourcePartition = extendedNetworkList.filter(tuple2 => {
      val found = tuple2._2.vertices.filter(vertex => vertex._1 == source).count() > 0
      if (found) true else false
    }).head

    // find target partition
    val targetPartition = extendedNetworkList.filter(tuple2 => {
      val found = tuple2._2.vertices.filter(vertex => vertex._1 == destination).count() > 0
      if (found) true else false
    }).head

    // check if source and target are in same partition
    val isBothVertexInSamePartition = if(sourcePartition._1 == targetPartition._1)  true else false

    val distance = isBothVertexInSamePartition match {
      case true => calculateDistanceSamePartition(targetPartition._2, source, destination)
      case false => calculateDistanceDifferentPartition(sourcePartition, targetPartition, source, destination)
    }

  }

  def calculateDistanceSamePartition(extendedGraph: Graph[Node, Int], source: VertexId, destination: VertexId): Int = {
    ShortestPath.singleSourceSingleTargetDijkstra(extendedGraph, source, destination)._2
  }

  def getNodeFromExtendedGrapg(extendedGraph: Graph[Node, Int], vertexId: VertexId): (VertexId, Node) = {
    extendedGraph.vertices.filter(vertex => vertex._1 == vertexId).collect().head
  }

  def calculateDistanceSourceBorderTargetBorder(sourceNode: (VertexId, Node), targetNode: (VertexId, Node)): Int = {
    val distance = CDM.filter(cmdEntry => {
      cmdEntry._1 == sourceNode._2.partition && cmdEntry._2 == targetNode._2.partition
    }).map(cmdEntry => {
      val result = cmdEntry._3.filter(listEntry => listEntry._1 == sourceNode._1 && listEntry._2 == targetNode._1).head
      result._3
    }).collect().head
    // return distance
    distance
  }

  def calculateDistanceSourceTargetBorder(sourceNode: (VertexId, Node), targetNode: (VertexId, Node)): Int = {
    // bu durumda sourceIDT yi broadcast yap ve CDM ile joinle bi sekilde
    val readyforBC = sourceNode._2.idt.get.borderIDTList.map(elem => (sourceNode._1, elem._1, elem._3))
    val sourceIDT_BC = sc.broadcast(readyforBC)

    // CDM'in sadece source target  row'unu aldim.
    val sourceTargetCDM = CDM.filter(cdmEntry => cdmEntry._1 == sourceNode._2.partition && cdmEntry._2 == targetNode._2.partition)

    // you can filter sourceTargetCMD for target only, because we know target node is border node
    // source:16L (C3) target:21L (C4) orneginden cmd'de listbufferin icinde (C3 borderlari, C4 borderleri, distance) seklinde veri tipi.
    // C4 border i zaten target oldugu icin, diger borderlari almama gerek yok, o yuzden filtreleme yapiyorum
    val CDMfilteredForTarget = sourceTargetCDM.map(entry => {
      entry._3.filter(elem => elem._2 == targetNode._1)
    })

    val CDM_Map: RDD[(VertexId, VertexId, Int)] = CDMfilteredForTarget.flatMap(elem => {
      elem.map(listItem => (listItem._1, listItem._2, listItem._3))
    })

    val distanceList = CDM_Map.flatMap{
      case(key, value1, value2) =>
        sourceIDT_BC.value.filter(item => item._2 == key).map{bcValues =>
          (bcValues._1, key, value1, bcValues._3("out") + value2)
        }
    }

    val distanceResult = distanceList.min()(
      new Ordering[(VertexId, VertexId, VertexId, Int)]() {
        override def compare(x: (VertexId, VertexId, VertexId, Int), y: (VertexId, VertexId, VertexId, Int)): Int = {
          Ordering[Int].compare(x._4, y._4)
        }
      }
    )._4

    println(distanceResult)
    distanceResult
  }

  def calculateDistanceSourceBorderTarget(sourceNode: (VertexId, Node), targetNode: (VertexId, Node)): Int = {
    // bu durumda target tarafini broadcast yapicam.
    val readyforBC = targetNode._2.idt.get.borderIDTList.map(elem => (targetNode._1, elem._1, elem._3))
    val targetIDT_BC = sc.broadcast(readyforBC)
    val sourceTargetCDM = CDM.filter(cdmEntry => cdmEntry._1 == sourceNode._2.partition && cdmEntry._2 == targetNode._2.partition)

    //
    val CDMfilteredForSource = sourceTargetCDM.map(entry => {
      entry._3.filter(elem => elem._1 == sourceNode._1)
    })

    val CDM_Map: RDD[(VertexId, VertexId, Int)] = CDMfilteredForSource.flatMap(elem => {
      elem.map(listItem => (listItem._1, listItem._2, listItem._3))
    })

    val distanceList = CDM_Map.flatMap{
      case(key, value1, value2) =>
        targetIDT_BC.value.filter(item => item._2 == key).map{bcValues =>
          (bcValues._1, key, value1, bcValues._3("in") + value2)
        }
    }

    val distanceResult = distanceList.min()(
      new Ordering[(VertexId, VertexId, VertexId, Int)]() {
        override def compare(x: (VertexId, VertexId, VertexId, Int), y: (VertexId, VertexId, VertexId, Int)): Int = {
          Ordering[Int].compare(x._4, y._4)
        }
      }
    )._4

    println(distanceResult)
    distanceResult
  }

  def calculateDistanceSourceTarget(sourceNode: (VertexId, Node), targetNode: (VertexId, Node)): Int = {
    val readyForSourceBC = sourceNode._2.idt.get.borderIDTList.map(elem => (sourceNode._1, elem._1, elem._3))
    val readyForTargetBC = targetNode._2.idt.get.borderIDTList.map(elem => (targetNode._1, elem._1, elem._3))
    val sourceIDT_BC = sc.broadcast(readyForSourceBC)
    val targetIDT_BC = sc.broadcast(readyForTargetBC)

    // CDM'in source parition - target partition olan row'unu aldim.
    val sourceTargetCDM = CDM.filter(cdmEntry => cdmEntry._1 == sourceNode._2.partition && cdmEntry._2 == targetNode._2.partition)

    // aslinda burda filter yapmadim sadece mapleyerek uygun formata getirdim (String String ListBuffer) i (ListBuffer) e cevirdim.
    val CDMfiltered =  sourceTargetCDM.map(elem => elem._3)

    // bu kodu sadece ListBuffer dan kurtarmak icin yazdik
    val CDM_Map: RDD[(VertexId, VertexId, Int)] = CDMfiltered.flatMap(elem => {
      elem.map(listItem => (listItem._1, listItem._2, listItem._3))
    })

    val distanceListTemp = CDM_Map.flatMap{
      case(key, value1, value2) =>
        sourceIDT_BC.value.filter(item => item._2 == key).map{bcValues =>
          (bcValues._1, key, value1, bcValues._3("out") + value2)
        }
    }

    val distanceList = distanceListTemp.flatMap{
      case (source, sourceBorder, key, distance) =>
        targetIDT_BC.value.filter(item => item._2 == key).map{bcValues =>
          (source, sourceBorder, key, bcValues._1, distance + bcValues._3("in"))
        }
    }

    println("tum distance list basilcak insallah")
    distanceList.collect().foreach(println(_))

    val distanceResult = distanceList.min()(
      new Ordering[(VertexId, VertexId, VertexId, VertexId, Int)]() {
        override def compare(x: (VertexId, VertexId, VertexId, VertexId, Int), y: (VertexId, VertexId, VertexId, VertexId, Int)): Int = {
          Ordering[Int].compare(x._5, y._5)
        }
      }
    )._5

    println("distance result dogru insallah")
    println(distanceResult)
    distanceResult


  }

  def calculateDistanceDifferentPartition(sourceExtendedGraph: (String, Graph[Node, Int]),
                                          targetExtendedGraph: (String, Graph[Node, Int]),
                                          source: VertexId,
                                          destination: VertexId): Unit = {

    // get source and target nodes
    val sourceNode = getNodeFromExtendedGrapg(sourceExtendedGraph._2, source)
    val targetNode = getNodeFromExtendedGrapg(targetExtendedGraph._2, destination)
    //val sourceNode = sourceExtendedGraph._2.vertices.filter(vertex => vertex._1 == source).collect().head
    //val targetNode = targetExtendedGraph._2.vertices.filter(vertex => vertex._1 == destination).collect().head

    // if both are border node, just return CDM
    val isSourceBorder = sourceNode._2.isBorderNode
    val isTargetBorder = targetNode._2.isBorderNode


    val distance = (isSourceBorder, isTargetBorder) match {
      case (true, true) => calculateDistanceSourceBorderTargetBorder(sourceNode, targetNode)
      case (false, true) => calculateDistanceSourceTargetBorder(sourceNode, targetNode)
      case (true, false) => calculateDistanceSourceBorderTarget(sourceNode, targetNode)
      case (false, false) => calculateDistanceSourceTarget(sourceNode, targetNode)
    }

    // we created calculateDistanceSourceBorderTargetBorder function for below
//    if (isSourceBorder && isTargetBorder) {
//      // means both are border so just look CDM
//      distance = CDM.filter(cmdEntry => {
//        cmdEntry._1 == sourceNode._2.partition && cmdEntry._2 == targetNode._2.partition
//      }).map(cmdEntry => {
//        val result = cmdEntry._3.filter(listEntry => listEntry._1 == sourceNode._1 && listEntry._2 == targetNode._1).head
//        result._3
//      }).collect().head
//    }

    println(distance)

    // if source is nonborder bur target is border
    // asagidaki kodu fonksiyon yaptik calculateDistanceSourceTargetBorder
//    if(isSourceBorder == false && isTargetBorder == true) {
//      // bu durumda sourceIDT yi broadcast yap ve CDM ile joinle bi sekilde
//      val readyforBC = (sourceNode._2.idt.get.borderIDTList.map(elem => (sourceNode._1, elem._1, elem._3)))
//      val sourceIDT_BC = sc.broadcast(readyforBC)
//
//      // CDM'in sadece source target  row'unu aldim.
//      val sourceTargetCDM = CDM.filter(cdmEntry => cdmEntry._1 == sourceNode._2.partition && cdmEntry._2 == targetNode._2.partition)
//
//      // you can filter sourceTargetCMD for target only, because we know target node is border node
//      // source:16L (C3) target:21L (C4) orneginden cmd'de listbufferin icinde (C3 borderlari, C4 borderleri, distance) seklinde veri tipi.
//      // C4 border i zaten target oldugu icin, diger borderlari almama gerek yok, o yuzden filtreleme yapiyorum
//      val CDMfilteredForTarget = sourceTargetCDM.map(entry => {
//        entry._3.filter(elem => elem._2 == targetNode._1)
//      })
//
//      val CDM_Map: RDD[(VertexId, VertexId, Int)] = CDMfilteredForTarget.flatMap(elem => {
//        elem.map(listItem => (listItem._1, listItem._2, listItem._3))
//      })
//
//      val distanceList = CDM_Map.flatMap{
//        case(key, value1, value2) =>
//          sourceIDT_BC.value.filter(item => item._2 == key).map{bcValues =>
//            (bcValues._1, key, value1, bcValues._3 + value2)
//          }
//      }
//
//      val distanceResult = distanceList.min()(
//        new Ordering[(VertexId, VertexId, VertexId, Int)]() {
//          override def compare(x: (VertexId, VertexId, VertexId, Int), y: (VertexId, VertexId, VertexId, Int)): Int = {
//            Ordering[Int].compare(x._4, y._4)
//          }
//        }
//      )._4
//
//      println(distanceResult)
//      distance = distanceResult
//
//    }

    // asagikdai bolumu de fonksyion yaptik calculateDistanceSourceBorderTarget
//    if(isSourceBorder == true && isTargetBorder == false) {
//      // bu durumda target tarafini broadcast yapicam.
//      val readyforBC = targetNode._2.idt.get.borderIDTList.map(elem => (targetNode._1, elem._1, elem._3))
//      val targetIDT_BC = sc.broadcast(readyforBC)
//      val sourceTargetCDM = CDM.filter(cdmEntry => cdmEntry._1 == sourceNode._2.partition && cdmEntry._2 == targetNode._2.partition)
//
//      //
//      val CDMfilteredForSource = sourceTargetCDM.map(entry => {
//        entry._3.filter(elem => elem._1 == sourceNode._1)
//      })
//
//      val CDM_Map: RDD[(VertexId, VertexId, Int)] = CDMfilteredForSource.flatMap(elem => {
//        elem.map(listItem => (listItem._1, listItem._2, listItem._3))
//      })
//
//      val distanceList = CDM_Map.flatMap{
//        case(key, value1, value2) =>
//          targetIDT_BC.value.filter(item => item._2 == key).map{bcValues =>
//            (bcValues._1, key, value1, bcValues._3 + value2)
//          }
//      }
//
//      val distanceResult = distanceList.min()(
//        new Ordering[(VertexId, VertexId, VertexId, Int)]() {
//          override def compare(x: (VertexId, VertexId, VertexId, Int), y: (VertexId, VertexId, VertexId, Int)): Int = {
//            Ordering[Int].compare(x._4, y._4)
//          }
//        }
//      )._4
//
//      println(distanceResult)
//      distance = distanceResult
//
//    }

    // asagikdaini de fonksiyon yaptik calculateDistanceSourceTarget
//    if(isSourceBorder == false && isTargetBorder == false) {
//      val readyForSourceBC = sourceNode._2.idt.get.borderIDTList.map(elem => (sourceNode._1, elem._1, elem._3))
//      val readyForTargetBC = targetNode._2.idt.get.borderIDTList.map(elem => (targetNode._1, elem._1, elem._3))
//      val sourceIDT_BC = sc.broadcast(readyForSourceBC)
//      val targetIDT_BC = sc.broadcast(readyForTargetBC)
//
//      // CDM'in source parition - target partition olan row'unu aldim.
//      val sourceTargetCDM = CDM.filter(cdmEntry => cdmEntry._1 == sourceNode._2.partition && cdmEntry._2 == targetNode._2.partition)
//
//      // aslinda burda filter yapmadim sadece mapleyerek uygun formata getirdim (String String ListBuffer) i (ListBuffer) e cevirdim.
//      val CDMfiltered =  sourceTargetCDM.map(elem => elem._3)
//
//      // bu kodu sadece ListBuffer dan kurtarmak icin yazdik
//      val CDM_Map: RDD[(VertexId, VertexId, Int)] = CDMfiltered.flatMap(elem => {
//        elem.map(listItem => (listItem._1, listItem._2, listItem._3))
//      })
//
//      val distanceListTemp = CDM_Map.flatMap{
//        case(key, value1, value2) =>
//          sourceIDT_BC.value.filter(item => item._2 == key).map{bcValues =>
//            (bcValues._1, key, value1, bcValues._3 + value2)
//          }
//      }
//
//      val distanceList = distanceListTemp.flatMap{
//        case (source, sourceBorder, key, distance) =>
//          targetIDT_BC.value.filter(item => item._2 == key).map{bcValues =>
//            (source, sourceBorder, key, bcValues._1, distance + bcValues._3)
//          }
//      }
//
//      println("tum distance list basilcak insallah")
//      distanceList.collect().foreach(println(_))
//
//      val distanceResult = distanceList.min()(
//        new Ordering[(VertexId, VertexId, VertexId, VertexId, Int)]() {
//          override def compare(x: (VertexId, VertexId, VertexId, VertexId, Int), y: (VertexId, VertexId, VertexId, VertexId, Int)): Int = {
//            Ordering[Int].compare(x._5, y._5)
//          }
//        }
//      )._5
//
//      println("distance result dogru insallah")
//      println(distanceResult)
//
//
//    }

    }

  def queryShortestPath(source: VertexId, destination: VertexId): Unit = {
    // find source partitiion.
    val sourcePartition = extendedNetworkList.filter(tuple2 => {
      val found = tuple2._2.vertices.filter(vertex => vertex._1 == source).count() > 0
      if (found) true else false
    }).head

    val targetPartition = extendedNetworkList.filter(tuple2 => {
      val found = tuple2._2.vertices.filter(vertex => vertex._1 == destination).count() > 0
      if (found) true else false
    }).head

    val isBothVertexInSamePartition = if(sourcePartition._1 == targetPartition._1)  true else false

    val shortestPath = isBothVertexInSamePartition match {
      case true => calculateShortestPathSamePartition(targetPartition._2, source, destination)
      case false => calculateShortestPathDifferentPartition(sourcePartition, targetPartition, source, destination)
    }
  }

  def calculateShortestPathSamePartition(extendedGraph: Graph[Node, Int], source: VertexId, destination: VertexId) = {
    ShortestPath.singleSourceSingleTargetDijkstra(extendedGraph, source, destination)._3
  }

  def calculateShortestPathDifferentPartition(sourceExtendedGraph: (String, Graph[Node, Int]),
                                              targetExtendedGraph: (String, Graph[Node, Int]),
                                              source: VertexId,
                                              destination: VertexId): Unit = {
    // burda once distance calistirmamiz lazim false false olani ve path i alicam,
    // daha sonra bu path in 1. ve 2. elemanini na shortest sonra 2. ve 3. elemanina
    // son olarkata 3. ve 4. elemanina shortest cekicen sonra bu gelen pathleri bi concatliyacan

    val sourceNode = getNodeFromExtendedGrapg(sourceExtendedGraph._2, source)
    val targetNode = getNodeFromExtendedGrapg(targetExtendedGraph._2, destination)


    val pathToQuery = findPathToQuery(sourceNode, targetNode)
    val query1 = runPartialShortestPathinEC(sourceExtendedGraph._2, pathToQuery._1, pathToQuery._2)
    val query2 = runPartialShortestPathinT(transitNetwork, pathToQuery._2, pathToQuery._3)
    val query3 = runPartialShortestPathinEC(targetExtendedGraph._2, pathToQuery._3, pathToQuery._4)

    val result1 = Await.result(query1, Duration.Inf)
    val result2 = Await.result(query2, Duration.Inf)
    val result3 = Await.result(query3, Duration.Inf)





  }

  def runPartialShortestPathinEC(graph: Graph[Node, Int], source: VertexId, destination: VertexId): Future[(VertexId, Int, List[VertexId])] = Future {
    log.info(s"thread started for shortest path source: $source and destination $destination")
    ShortestPath.singleSourceSingleTargetDijkstra(graph, source, destination)
  }

  def runPartialShortestPathinT(graph: Graph[VertexId, Int], source: VertexId, destination: VertexId): Future[(VertexId, Int, List[VertexId])] = Future {
    log.info(s"thread started for shortest path source: $source and destination $destination")
    ShortestPath.singleSourceSingleTargetDijkstra(graph, source, destination)

  }

  def findPathToQuery(sourceNode: (VertexId, Node), targetNode: (VertexId, Node)): (VertexId, VertexId, VertexId, VertexId) = {
    val readyForSourceBC = sourceNode._2.idt.get.borderIDTList.map(elem => (sourceNode._1, elem._1, elem._3))
    val readyForTargetBC = targetNode._2.idt.get.borderIDTList.map(elem => (targetNode._1, elem._1, elem._3))
    val sourceIDT_BC = sc.broadcast(readyForSourceBC)
    val targetIDT_BC = sc.broadcast(readyForTargetBC)

    // CDM'in source parition - target partition olan row'unu aldim.
    val sourceTargetCDM = CDM.filter(cdmEntry => cdmEntry._1 == sourceNode._2.partition && cdmEntry._2 == targetNode._2.partition)

    // aslinda burda filter yapmadim sadece mapleyerek uygun formata getirdim (String String ListBuffer) i (ListBuffer) e cevirdim.
    val CDMfiltered =  sourceTargetCDM.map(elem => elem._3)

    // bu kodu sadece ListBuffer dan kurtarmak icin yazdik
    val CDM_Map: RDD[(VertexId, VertexId, Int)] = CDMfiltered.flatMap(elem => {
      elem.map(listItem => (listItem._1, listItem._2, listItem._3))
    })

    val distanceListTemp = CDM_Map.flatMap{
      case(key, value1, value2) =>
        sourceIDT_BC.value.filter(item => item._2 == key).map{bcValues =>
          (bcValues._1, key, value1, bcValues._3("out") + value2)
        }
    }

    val distanceList = distanceListTemp.flatMap{
      case (source, sourceBorder, key, distance) =>
        targetIDT_BC.value.filter(item => item._2 == key).map{bcValues =>
          (source, sourceBorder, key, bcValues._1, distance + bcValues._3("in"))
        }
    }

    println("tum distance list basilcak insallah")
    distanceList.collect().foreach(println(_))

    val distancePath = distanceList.min()(
      new Ordering[(VertexId, VertexId, VertexId, VertexId, Int)]() {
        override def compare(x: (VertexId, VertexId, VertexId, VertexId, Int), y: (VertexId, VertexId, VertexId, VertexId, Int)): Int = {
          Ordering[Int].compare(x._5, y._5)
        }
      }
    )

    (distancePath._1, distancePath._2, distancePath._3, distancePath._4)


  }















}

object QueryProcessor {

  def apply(extendedNetworkList: Array[(String, Graph[Node, Int])],
            transitNetwork:  Graph[VertexId, Int],
            CDM: RDD[(String, String, ListBuffer[(VertexId, VertexId, Int)])],
            sc: SparkContext,
            paralel: Int = 3): QueryProcessor = {
    val ec = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(paralel, new CustomThreadFactory))
    new QueryProcessor(extendedNetworkList, transitNetwork, CDM, sc, ec)
  }


}
