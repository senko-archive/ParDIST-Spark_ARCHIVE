package com.senko.ParDISTonSpark

import java.io
import java.util.concurrent.Executors

import org.apache.log4j.LogManager
import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, EdgeTriplet, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

class PreProcessor(graph: Graph[Node,Int], parallel: Int,  ec: ExecutionContext, sc: SparkContext) {
  val log = LogManager.getLogger(this.getClass)
  implicit val ec_inner = ec
  val parallelWorkerNumber = parallel
  val scinner: SparkContext = sc

  def prepareExtendedComponents() = {

    val partitions = GraphHelpers.getPartitions(graph)
    log.info(s"Graph have ${partitions.length} partitions")
    log.info(s"partitions are: ${partitions.mkString(" - ")}")

    val futureResults = partitions.map(startCreateEC(_))
    val result = futureResults.map(elem => {
      Await.result(elem, Duration.Inf)
    })

    // combine all partial transit network info from partitions.
    var transitNetworkVertices = new ListBuffer[VertexId]()
    var transitNetworkEdges = new ListBuffer[Edge[Int]]()
    for(partition <- result) {
      val aa: ListBuffer[VertexId] = partition._3
      transitNetworkVertices = transitNetworkVertices ++ aa
      val bb: ListBuffer[Edge[Int]] = partition._4
      transitNetworkEdges = transitNetworkEdges ++ bb
    }

    //transitNetworkVertices = transitNetworkVertices.distinct
    var newtransitNetworkVertices = new ListBuffer[(VertexId, VertexId)]
    newtransitNetworkVertices = transitNetworkVertices.distinct.map(elem => (elem, elem))
    transitNetworkEdges = transitNetworkEdges.distinct

    val transitNetworkVerticesRDD = sc.parallelize(newtransitNetworkVertices)
    val transitNetworkEdgesRDD = sc.parallelize(transitNetworkEdges)

    val transitNetworkGraph = Graph(transitNetworkVerticesRDD, transitNetworkEdgesRDD)
    val extendedComponentArray = result.map(result => (result._1, result._2))

    // return all extended component subgraphs and transit network graph
    (extendedComponentArray, transitNetworkGraph)

  }

  def prepareComponentDistanceMatrix() = {
    val partitions = GraphHelpers.getPartitions(graph)
    val CDM = new ListBuffer[(String, String, ListBuffer[(VertexId, VertexId, Int)])]
    val vertexList = new ListBuffer[(String, List[VertexId])]

    for(partition <- partitions) {
      val vertices = graph.vertices.filter(vertex => vertex._2.partition == partition && vertex._2.isBorderNode == true).collect()
      vertexList.append((partition, vertices.map(vertex => vertex._1).toList))
    }

    // loop for inside a partition for each partition
    for(sourceVertexGroup <- vertexList) {
      for(destVertexGroup <- vertexList) {
        log.info(s"CDM calculating for source partition: ${sourceVertexGroup._1} destination partition: ${destVertexGroup._1}")
        if (sourceVertexGroup._1 != destVertexGroup._1) {

          val tempVertexList = new ListBuffer[(VertexId, VertexId, Int)]

          // for vertexID in partition to each vertexID in other partition
          for(sourceVertex <- sourceVertexGroup._2) {
            for(destVertex <- destVertexGroup._2) {
              log.debug(s"shortest path is calculating for, source: ${sourceVertex} destination: ${destVertex}")
              log.info(s"shortest path is calculating for, source: ${sourceVertex} destination: ${destVertex}")
              val result = ShortestPath.singleSourceSingleTargetDijkstra(graph, sourceVertex, destVertex)
              log.debug(s"shortest path calculation completed for, source: ${sourceVertex} destination: ${destVertex}")
              log.info(s"shortest path calculation completed for, source: ${sourceVertex} destination: ${destVertex}")
              tempVertexList.append((sourceVertex, destVertex, result._2))
            }
          }

          CDM.append((sourceVertexGroup._1, destVertexGroup._1, tempVertexList))

        }
      }
    }
    // return component distance matrix
    CDM

  }

  def startCreateEC(partitionName: String) = Future {

    log.info(s"thread started for partition: ${partitionName} with thread: ${Thread.currentThread().getName}")

    // get border and non-border nodes for partition
    val partitionNodes = GraphHelpers.getPartitionNodes(graph, partitionName)
    val borderNodes: Array[(VertexId, String)] = partitionNodes.filter(vertex => vertex._2.isBorderNode).map(vertex => (vertex._1, vertex._2.name)).collect()
    val nonBorderNodes: Array[(VertexId, String)] = partitionNodes.filter(vertex => !vertex._2.isBorderNode).map(vertex => (vertex._1, vertex._2.name)).collect()

    // 2. idt icin source: border node, dest: other all non-border nodes
    log.info(s"IDT is calculating for partition ${partitionName}")
    val IDT_map: Map[VertexId, IDT] = computeIDT(borderNodes, nonBorderNodes)


    // 3. border node to border node shortest pathleri bul
    log.info(s"border to border shortest path are running... for partition: ${partitionName}")
    val borderCoupleList = GraphHelpers.generateBorderCouples(borderNodes)
    val borderToBorderShortest: ListBuffer[(VertexId, String, VertexId, String, List[VertexId])] = borderCoupleList.map(item => {
      // sourceId, sourceName, targetId, targetName,
      (item._1._1, item._1._2, item._2._1, item._2._2,
        ShortestPath.singleSourceSingleTargetDijkstra(graph, item._1._1, item._2._1)._3 :+ item._2._1)
    })

    // getPartitionGraph returns partitions node and vertices as graph
    // for extended component you need to add vertices and edges from border to border shortest paths
    log.info(s"extended graph is extracting from original graph for partition: ${partitionName}")
    val extendedSubGraph = GraphHelpers.getPartitionGraph(graph, partitionName)

    // burda extendedSubGraph ile IDT joinle
    log.info(s"IDT data joining with extended graph for partition: ${partitionName}")
    val IDT_RDD: RDD[(VertexId, IDT)] = sc.parallelize(IDT_map.toList)
    val ecWithIDT = extendedSubGraph.joinVertices[IDT](IDT_RDD)((VertexID, VD, U) => {
      Node(VD.name, VD.isBorderNode, VD.partition, Some(U))
    })


    val verticesToAdd: ListBuffer[(VertexId, Node)] = findVerticesToAddEC(borderToBorderShortest, extendedSubGraph)
    val edgeIndexCouples: ListBuffer[(VertexId, VertexId)] = getEdgeIndexCouples(borderToBorderShortest)

    val edgesToAdd: ListBuffer[Edge[Int]] = findEdgesToAddEC(edgeIndexCouples)

    val verticesToAddRDD = sc.parallelize(verticesToAdd)
    val edgesToAddRDD = sc.parallelize(edgesToAdd)

    val unionedVertices = ecWithIDT.vertices.union(verticesToAddRDD)
    val unionedEdges = ecWithIDT.edges.union(edgesToAddRDD).distinct()

    log.info(s"updated extended graph is creating... for partition: ${partitionName}")
    val newExtendedComponent = Graph(unionedVertices, unionedEdges)

//    log.info(s"for test for partition: ${partitionName}")
//    newExtendedComponent.vertices.foreach(println(_))
//    newExtendedComponent.vertices.foreach(vertex => vertex._2)
//    newExtendedComponent.edges.foreach(println(_))

    val (transitNetworkVertices, transitNetworkEdges) = createPartitionOfTransitNetwork(borderToBorderShortest, edgesToAdd)

    // return map of partitionName, extendedNetwork, transitNetworkEdges and transitNetworkVertices
    (partitionName, newExtendedComponent, transitNetworkVertices, transitNetworkEdges)

  }

  def computeIDT(borderNodes: Array[(VertexId, String)], nonBorderNodes: Array[(VertexId, String)]) = {
    val IDT_mid_result = borderNodes.flatMap(borderNode => {
      nonBorderNodes.map(nonBorderNode => {
        log.debug(s"shortestpath is running for source: ${borderNode._1} and destination: ${nonBorderNode._1}")
        val shortestPathResult = ShortestPath.singleSourceSingleTargetDijkstra(graph, borderNode._1, nonBorderNode._1)
        // tuple is source_id, source_name, destination_id, destinatoin_name, path_cost
        val result = (borderNode._1, borderNode._2, nonBorderNode._1, nonBorderNode._2, shortestPathResult._2)
        result
      })
    })

    // below code returns map of tuples (nonBorderNode_ID, IDT)
    val IDT_result: Map[VertexId, IDT] = IDT_mid_result.groupBy(_._3).map(elem => {
      var result: Array[(VertexId, String, Int)] = elem._2.map(inside => (inside._1, inside._2, inside._5))
      var lb = new ListBuffer[(VertexId, String, Int)]()
      result.foreach(item => {
        val toInsert = (item._1, item._2, item._3)
        lb.append(toInsert)
      })
      (elem._1, IDT(lb))
    })
    IDT_result
  }

  /*
  returns edge couples in shortest path list
  if list is [A,D,E,F,C] then returns (A,D),(D,E)(E,F)...
   */
  def getEdgeIndexCouples(borderToBorderShortest: ListBuffer[(VertexId, String, VertexId, String, List[VertexId])]) = {
    val edgeIndexCouples =new ListBuffer[(VertexId, VertexId)]()
    borderToBorderShortest.foreach(item => {
      val shortestPath = item._5
      for(i<-0 to shortestPath.length-2) {
        edgeIndexCouples.append((shortestPath(i), shortestPath(i+1)))
      }
    })
    edgeIndexCouples
  }

  def findVerticesToAddEC(borderToBorderShortest: ListBuffer[(VertexId, String, VertexId, String, List[VertexId])], extendedSubGraph: Graph[Node, Int]) = {
    val verticesToAdd = new ListBuffer[(VertexId, Node)]()

    borderToBorderShortest.foreach(item => {
      item._5.foreach(vertexID => {
        // look if vertexID is in this component
        if (extendedSubGraph.vertices.lookup(vertexID).isEmpty) {
          // if is empty then this vertex ID is from another component
          val vertex = graph.vertices.lookup(vertexID)
          verticesToAdd.append((vertexID, vertex.head))
        }
      })
    })
    verticesToAdd
  }

  def findEdgesToAddEC(edgeIndexCouples: ListBuffer[(VertexId, VertexId)]) = {
    val edgesToAdd = new ListBuffer[Edge[Int]]
    edgeIndexCouples.foreach(item => {
      // IMPORTANT -> here if vertices are A D E I'm taking edge between A->D, D->A, D->E, E->D
      val toADD: Array[EdgeTriplet[Node, Int]] = graph.triplets.filter(triplet => (triplet.srcId == item._1 && triplet.dstId == item._2) || (triplet.srcId == item._2 && triplet.dstId == item._1)).collect()
      toADD.foreach(triplet => {
        val edge = Edge(triplet.srcId.toLong, triplet.dstId.toLong, triplet.attr)
        edgesToAdd.append(edge)
      })
    })
    edgesToAdd
  }

  def createPartitionOfTransitNetwork(borderToBorderShortest: ListBuffer[(VertexId, String, VertexId, String, List[VertexId])],
                                      edgesToAdd: ListBuffer[Edge[Int]]) = {
    // I can follow 2 different approch, one is create Gt in partitions as a list and hold information in driver
    // second is for each partition create small Gt Graph and at the end union them
    // for now I'm following first one.

    /*
    for gt first I need to find connections between two border nodes. (It may included in extended network of partition)
    like in paper example graph -> for partiton1 in extended network we already included node n6 and n7
    but not n8, which is connected with n4.
    */

    // first find border nodes triplets which have connections to another partition
    val allVertexIDs = borderToBorderShortest.flatMap(shortest => shortest._5).toSet
    val allVertexIDsMutable = collection.mutable.ListBuffer(allVertexIDs.toSeq: _*)

    val transitNetworkEdges = new ListBuffer[Edge[Int]]()
    val transitNetworkVertices = new ListBuffer[VertexId]()

    allVertexIDs.foreach(vertexID => {
      val neighbour = graph.triplets.filter(triplet => {
        triplet.srcId == vertexID && triplet.srcAttr.isBorderNode == true && triplet.srcAttr.partition != triplet.dstAttr.partition
      }).collect()
      neighbour.foreach(neig => allVertexIDsMutable.append(neig.dstId))
      neighbour.foreach(neig => transitNetworkEdges.append(Edge(neig.srcId, neig.dstId, neig.attr)))
      neighbour.foreach(neig => transitNetworkEdges.append(Edge(neig.dstId, neig.srcId, neig.attr)))
    })

    transitNetworkVertices.append(allVertexIDsMutable: _*)
    transitNetworkEdges.append(edgesToAdd: _*)

    (transitNetworkVertices, transitNetworkEdges)
  }
}

object PreProcessor {

  def apply(graph: Graph[Node,Int], paralel:Int, sc: SparkContext) = {
    val ec = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(paralel, new CustomThreadFactory))
    new PreProcessor(graph, paralel, ec, sc)
  }

}




