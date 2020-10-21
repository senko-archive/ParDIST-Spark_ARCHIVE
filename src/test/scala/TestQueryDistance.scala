import com.senko.ParDISTonSpark.Node
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{Edge, Graph}

object TestQueryDistance extends App{

  val vertexN0 = (1L, Node("n0", false, "C1", None))
  val vertexN1 = (2L, Node("n1", false, "C1", None))
  val vertexN2 = (3L, Node("n2", true, "C1", None))

  val edge12 = Edge(1L, 2L,10)
  val edge13 = Edge(1L, 3L, 20)

  val vertexN6 = (7L, Node("n6", true, "C2", None))
  val vertexN7 = (8L, Node("n7", true, "C2", None))
  val vertexN9 = (9L, Node("n9", false, "C2", None))

  val edge67 = Edge(7L, 8L, 4)
  val edge79 = Edge(8L, 9L, 5)

  val seqOfVertices_g1 = Seq(vertexN0, vertexN1, vertexN2)
  val seqOfEdges_g1 = Seq(edge12, edge13)

  val seqOfVertices_g2 = Seq(vertexN6, vertexN7, vertexN9)
  val seqOfEdges_g2 = Seq(edge67, edge79)

  val conf = new SparkConf().setAppName("GraphDeneme").setMaster("local[*]")
  val sc = new SparkContext(conf)

  val g1_vertices = sc.parallelize(seqOfVertices_g1)
  val g2_vertices = sc.parallelize(seqOfVertices_g2)
  val g1_edges = sc.parallelize(seqOfEdges_g1)
  val g2_edges = sc.parallelize(seqOfEdges_g2)

  val graph1 = Graph(g1_vertices, g1_edges)
  val graph2 = Graph(g2_vertices, g2_edges)

  val array = Array[(String, Graph[Node, Int])](("A", graph1), ("B", graph2))

  val result = array.filter(elem => {
    val is_in_partition = elem._2.vertices.filter(vertex => vertex._1 == 7L).count > 0
    println(elem)
    println(is_in_partition)
    if (is_in_partition) true
    else false
  }).head._1

  println("-------------")
  println(result)

  val target = array.filter(element => element._1 == result).head
  val secondResult = target._2.vertices.filter(vertex => (vertex._1 == 1L && vertex._2.partition == result)).count() > 0

  println("........")
  println(secondResult)

  secondResult match {
    case true => println("EVED")
    case false => println("HAYIR")
  }




}
