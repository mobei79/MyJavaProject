package com.fengjr.NeographX

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object AppgrammarTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("Neo4jGraphX").setMaster("local[*]")
//      .set("spark.neo4j.bolt.url","bolt://10.10.203.132:7687")
//      .set("spark.neo4j.bolt.user","neo4j")
//      .set("spark.neo4j.bolt.password","kgfengjr")
    val sc = new SparkContext(conf)
//    val neo = Neo4j(sc)

    // 图graph 的创建 方法1
    val nodes: RDD[(VertexId,(Int,Int))] = sc.parallelize(Array(    //创建节点RDD
      (5109594L,(1,1)),(4262387L,(1,0)),
      (4849603L,(1,1)),(4968188L,(0,0))
    ))
    val relationships: RDD[Edge[Int]] = sc.parallelize(Array(   //创建边RDD
      Edge(5109594L,4262387L,1),Edge(5109594L,4849603L,2),
      Edge(4262387L,4849603L,2),Edge(5109594L,4968188L,5)
    ))
    val defaultnode = (2,2)  // 定义一个默认用户，避免有不存在用户的关系
    val graphyj = Graph(nodes,relationships,defaultnode)       // 构造Graph
    // 图graph 的创建 方法2

    // 节点RDD和边RDD的过滤
    val fcount1 = graphyj.vertices.filter{
      case (id,(end_result,dpd7)) => dpd7 == 1
    }.count
    val fcount2 = graphyj.edges.filter(edge => edge.srcId > edge.dstId).count
    val fcount3 = graphyj.edges.filter{
      case Edge(src,dst,props) => src<dst
    }.count

    // Triplets 三元组，包含源点，源点属性，目标节点，目标节点属性，边属性
    val triplets = graphyj.triplets
    triplets.collect().foreach(println(_))
    val triplets1 = graphyj.triplets.map(triplet => triplet.srcId + "-" + triplet.srcAttr._1 + "-" + triplet.srcAttr._2
      + "-" +  triplet.dstId + "-" + triplet.dstAttr._1 + "-" + triplet.dstAttr._2 + triplet.attr
    )
    triplets1.collect().foreach(println(_))

    //  度，入度，出度
    val degrees: VertexRDD[Int] = graphyj.degrees
    degrees.collect().foreach(println)
    val indegrees: VertexRDD[Int] = graphyj.inDegrees
    val outdegrees: VertexRDD[Int] = graphyj.outDegrees
    outdegrees.collect().foreach(println)

    // 构建子图 可以用于提取子图
    val subgraph = graphyj.subgraph(vpred = (id,attr) => attr._2 != 1)
    subgraph.vertices.collect().foreach(println(_))

    // Map操作，根据原图的一些特性得到新图，原图结构是不变的。下面两个逻辑是等价的，但是第一个不会被Graphx系统优化
    val newVertices = graphyj.vertices.map{
      case (id,attr) => (id,(attr._1 + "-1" ,attr._2 + "-2"))
    }
    val newGraph1 = Graph(newVertices,graphyj.edges)
    val newGraph2 = graphyj.mapVertices((id,attr) => (id,(attr._1 + "-1" ,attr._2 + "-2")))

    // 构造一个新图，顶点属性是出度
//    val inputGraph: Graph[Int,String] =
//      graphyj.outerJoinVertices(graphyj.outDegrees)((vid, _,degOpt) => degOpt.getOrElse(0))
//    val outputGraph: Graph[Double, Double] =
//      inputGraph.mapTriplets(triplet => 1.0 / triplet.srcAttr).mapVertices((id, _) => 1.0)

    // 图的反向操作，新的图形的所有边的方向相反，不修改顶点和边的属性，不改变边的数目，可以有效地实现不必要的数据移动和复制
    val rGraph = graphyj.reverse

    // Mask 操作也是根据输入图构造一个新图，达到一个限制约束的效果
    val ccGraph = graphyj.connectedComponents()
    val validGraph = graphyj.subgraph(vpred = (id,attr) => attr._2 != 1)
    val validCCGraph = ccGraph.mask(validGraph)

    // join 操作，原图外联出度点构成的一个新图，出度为顶点的属性
    val degreesGraph2 = graphyj.outerJoinVertices(outdegrees){
      (id,attr,outDegreeOpt) =>
        outDegreeOpt match {
          case Some(outDeg) => outDeg
          case None => 0
        }
    }

    // 缓存，默认情况下，缓存在内存的图会在内存紧张的时候被强制清理，采用的是LRU算法
    graphyj.cache()
    graphyj.persist(StorageLevel.MEMORY_ONLY)
    graphyj.unpersistVertices(true)

    //GraphLoader构建Graph
    var path = "/user/hadoop/data/temp/graph/graph.txt"
    var minEdgePartitions = 1
    var canonicalOrientation = false // if sourceId < destId this value is true
    val graph1 = GraphLoader.edgeListFile(sc, path, canonicalOrientation, minEdgePartitions,
      StorageLevel.MEMORY_ONLY, StorageLevel.MEMORY_ONLY)
    val verticesCount = graph1.vertices.count
    println(s"verticesCount: $verticesCount")
    graph1.vertices.collect().foreach(println)

    val edgesCount = graph1.edges.count
    println(s"edgesCount: $edgesCount")
    graph1.edges.collect().foreach(println)

    //PageRank
    val pageRankGraph = graph1.pageRank(0.001)
    pageRankGraph.vertices.sortBy(_._2, false).saveAsTextFile("/user/hadoop/data/temp/graph/graph.pr")
    pageRankGraph.vertices.top(5)(Ordering.by(_._2)).foreach(println)

    //Connected Components
    val connectedComponentsGraph = graph1.connectedComponents()
    connectedComponentsGraph.vertices.sortBy(_._2, false).saveAsTextFile("/user/hadoop/data/temp/graph/graph.cc")
    connectedComponentsGraph.vertices.top(5)(Ordering.by(_._2)).foreach(println)

    //TriangleCount主要用途之一是用于社区发现 保持sourceId小于destId
    val graph2 = GraphLoader.edgeListFile(sc, path, true)
    val triangleCountGraph = graph2.triangleCount()
    triangleCountGraph.vertices.sortBy(_._2, false).saveAsTextFile("/user/hadoop/data/temp/graph/graph.tc")
    triangleCountGraph.vertices.top(5)(Ordering.by(_._2)).foreach(println)

    sc.stop()



//    val allData = sc.textFile("allData.txt")
//    val partData = sc.textFile("partData.txt")
//    // partData是allData的子集
//    // 取partData的补集
//    // RDD.collect()尽量少用，如果单机允许的话，就可以用
//    val collect = partData.collect()
//    allData.filter(!collect.contains(_)).foreach(println)

      /**
        * 基本的RDD操作
        */


  }

    def basicTransformRDD(path:String):Unit={

    }



}
