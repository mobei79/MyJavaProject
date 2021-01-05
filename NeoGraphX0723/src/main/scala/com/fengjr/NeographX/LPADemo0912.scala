package com.fengjr.NeographX

import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.neo4j.spark.Neo4j

object LPADemo0912 {

  /**
    * 定义函数
    * @param args
    */
  def partitionsFun(/*index : Int,*/iter : Iterator[(String,String)]) : Iterator[String]= {
    var woman = List[String]()
    while (iter.hasNext){
      val next = iter.next()
      next match {
        case (_,"female") => woman = /*"["+index+"]"+*/next._1 :: woman
        case _=>
      }
    }
    return woman.iterator
  }

  /**
    * LabelPropagation
    */

  /**
    * main Function
    * @param args
    */
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("Neo4jGraphX")
      .setMaster("local[*]")    // 本地运行参数配置，提交集群时注释掉。
      .set("spark.neo4j.bolt.url","bolt://10.10.204.24:7687")
      .set("spark.neo4j.bolt.user","neo4j")
      .set("spark.neo4j.bolt.password","kgfengjr")
    val sc = new SparkContext(conf)
    val neo = Neo4j(sc)

    //  从Neo4j中读取数据； 使用janusgraph 结合Spark进行离线计算效果更好；Neo4j+spark 可以做成异步的计算工具，支持一些简单指标的计算
    val neodata = neo.cypher("MATCH (p1:real_income_no) -[:HasPhone]-> (ph1:phone) -[f:HasCall]- (ph2:phone) <-[:HasPhone]- (p2:real_income_no)" +
      "RETURN id(p1) as source, id(p2) as target, f.call_cnt as weight," +
      "p1.end_result as end_result1, case when (p1.dpd7 = '1' or p1.dpd15 = '1' or p1.dpd30 = '1' or p1.dpd90 ='1') then 1 else 0 end," +
      "p2.end_result as end_result2,  case when (p2.dpd7 = '1' or p2.dpd15 = '1' or p2.dpd30 = '1' or p2.dpd90 ='1') then 1 else 0 end limit 30").loadRowRdd

    // 得到allNode ：存储所有节点，以及节点的属性； 并生成节点NodeRDD
    val allNode = neodata.map(    // 如果不是对称关系的话，就需要取出p1和p2，去重，作为allNode；否则只需要取一侧的节点，去重即可。
      x => (x(0),x(3),x(4)))
      .union(neodata.map(
        x => (x(1),x(5),x(6))
      )
      )
//    println("此图中所有的节点数目是多少" + allNode.count().toString)
//    println("此图中所有的节点数目quchon是多少" + allNode.distinct().count().toString)  // 打印所有的节点信息allNode.distinct.foreach(println(_))   去重 allNode.distinct.foreach(println(_))
// allNode 存储所有节点，以及节点的属性； 并生成节点NodeRDD
//    val allNode = neodata.map(    // 如果不是对称关系的话，就需要取出p1和p2，去重，作为allNode；否则只需要取一侧的节点，去重即可。
//      x => (x(0),x(3),x(4))
//      )
    println("-------------此图中所有的节点数目是多少:" + allNode.distinct().count().toString + "-------------")  // 打印所有的节点信息allNode.distinct.foreach(println(_))   去重 allNode.distinct.foreach(println(_))
    //得到nodeRdd 和 relationRdd： 为节点RDD，存储网络中的所有关系
    val nodeRdd:RDD[(VertexId,(String,String))] = allNode.distinct.map(
      x => (
        x._1.toString.toLong,
        (x._2.toString,x._3.toString)
      )
    )
    println("-------------所有节点的RDD如下所示:nodeRdd-------------" )
    nodeRdd.top(100).foreach(println(_))

    val relationRdd:RDD[Edge[String]] = neodata.map(x =>(Edge(x(0).toString.toLong,x(1).toString.toLong,x(2).toString)) )
    println("-------------所有关系的RDD如下所示:relationRdd-------------" )
    relationRdd.take(40).foreach(println(_))
//    val defaultUser = ("2","3","4","5", "6") //  定义一个默认属性，避免有不存在用户的关系

    // 得到Graph ：使用Graph方法，构造图garph
    val graph = Graph(nodeRdd, relationRdd)
    graph.vertices.top(100).foreach(println(_))
    val nodes = graph.vertices

    val LpaGraph = Labelpropagation.run(graph,10)//lib.LabelPropagation.run(graph,10)
    // 得到LpaGraph：调用spark.graphx.lib中的LabelPropagation算法包 得到新图
//    val LpaGraph = lib.LabelPropagation.run(graph,10)
//    LpaGraph.vertices.top(10).foreach(println(_))
    LpaGraph.triplets.map(
      triplet =>
        triplet.srcId +"-"+ triplet.srcAttr + "|" + triplet.attr + "|" + triplet.dstId +"-"+ triplet.dstAttr
    ).top(40).foreach(println(_))
    println("-------------以上是运行LPA算法之后的新图的triplet-------------" )

    /**
      * 旧的方法
      */
    //    // 存储社区划分结果，（不带属性的结果）
//    val LpaNodes = LpaGraph.vertices    // (5411584,5411584) 即(Vid , Cid)
//    val LpaEdges = LpaGraph.edges
//    val Lparesult = LpaNodes.map(x => (x._2.toLong ,x._1.toLong)).groupByKey
//    Lparesult.take(100).foreach(println(_))   //(6311410,CompactBuffer(2701784, 4451802, 80217))
////    Lparesult.saveAsTextFile("output0911p1")
//    println("-------------存储社区划分结果，（不带属性的结果）格式为(6311410,CompactBuffer(2701784, 4451802, 6311410, 4356646, 3225776, 6032975, 4437285, 6334659, 2280217))-------------" )
//
//    // 统计各个社区的顶点个数
//    val Lpan = LpaNodes.join(nodes)   // (5411584,(5411584,(1,0))) 即 （Vid ，（Cid ，Pid））
//    Lpan.top(100).foreach(println(_))
//    val LpaRstFull = Lpan.map(t => (t._2._1,1)).reduceByKey(_+_)
//    LpaRstFull.take(100).foreach(println(_))
//    println("-------------统计各个社区的顶点个数-------------" )
//    Lpan.saveAsTextFile("output0911p2")

//    // 统计各个社区中未通过的个数  (6311410,9) 即（Cid ， num）
//    val LpaRstUnPass = Lpan.filter(t => t._2._2._1 == "1").map(x =>(x._2._1,1)).reduceByKey(_+_)
//    LpaRstUnPass.take(100).foreach(println(_))
//    println("-------------统计各个社区中未通过的个数-------------" )
//
//    // 统计各个社区中通过且逾期
//    val LpaRstPassOverdue = Lpan.filter(t => (t._2._2._1 == "2" && t._2._2 =="1")).map(x =>(x._2._1,1)).reduceByKey(_+_)
//    LpaRstPassOverdue.take(100).foreach(println(_))
//    println("-------------统计各个社区中通过且逾期-------------" )
//
//    val fuul1 = LpaRstFull.cogroup(LpaRstUnPass).cogroup(LpaRstPassOverdue)
////        .map(x => (x._1,x._2._1++x._2._2)).cogroup(LpaRstPassOverdue).map(y => (y._1,y._2._1++y._2._2))   a
//    fuul1.collect().foreach(println(_))
//    println("-------------统计各个社区中总数，未通过的个数，通过预期的个数-------------" )
////    fuul1.saveAsTextFile("output0911p2")
    /**
      * 新的map方法
      */
    // 存储社区划分结果，（不带属性的结果）
    val LpaNodes = LpaGraph.vertices    // (5411584,5411584) 即(Vid , Cid)
    val LpaEdges = LpaGraph.edges
    val Lparesult = LpaNodes.map(x => (x._2.toLong ,x._1.toLong)).groupByKey
    Lparesult.take(100).foreach(println(_))   //(6311410,CompactBuffer(2701784, 4451802, 80217))
//    Lparesult.saveAsTextFile("output0911p1")
    println("-------------存储社区划分结果，（不带属性的结果）格式为(6311410,CompactBuffer(2701784, 4451802, 6311410, 4356646, 3225776, 6032975, 4437285, 6334659, 2280217))-------------" )

    val Lpan = LpaNodes.join(nodes)   // (5411584,(5411584,(1,0))) 即 （Vid ，（Cid ，Pid））
    Lpan.top(100).foreach(println(_))
    val LpaRstFull = Lpan.map(t => (t._2._1,1)).reduceByKey(_+_)
    LpaRstFull.take(100).foreach(println(_))
    println("-------------统计各个社区的顶点个数-------------" )

    val communityUnPass = Lpan.map( line => {
      if (line._2._2._1 == "1")
        (line._2._1,1)
      else
        (line._2._1,0)
    }).reduceByKey(_+_)
    communityUnPass.take(100).foreach(println(_))
    println("-------------统计各个社区中未通过的个数-------------" )

    val communityPassOverdue = Lpan.map( line => {
      if (line._2._2._1 == "0" && line._2._2._2 == "1")
        (line._2._1,1)
      else
        (line._2._1,0)
    }).reduceByKey(_+_)
    communityPassOverdue.take(100).foreach(println(_))
    println("-------------统计各个社区中通过且逾期-------------" )

    val all1 = LpaRstFull.join(communityUnPass)join(communityPassOverdue)
    val aaaa = all1.map(x => (x._1, x._2._1._1,x._2._1._2,x._2._2))
    aaaa.take(100).foreach(println(_))
//    aaaa.saveAsTextFile("output0911p2")
//    val all12 = communityUnPass.join(LpaRstFull)
//    all12.take(100).foreach(println(_))
    sc.stop()
  }
}
