package com.fengjr.NeographX

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.neo4j.spark.Neo4j

object comGraphProperty {

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
    * getGraphMsg() 函数主要用于计算 复杂社交网络graph的 一些基本指标
    * @param graph
    */
  def getGraphMsg(graph:Graph[Int,Int]): Unit = {

    val startTime=new Date()
    val nodes = graph.vertices
    val N =  nodes.count    // 节点个数

    val relations = graph.edges
    val L = relations.count   //网络边数

    val degrees = graph.degrees
    var aveDegree = 0.0
    if(N != 0)
      aveDegree = degrees.map(x => x._2).reduce(_+_).toDouble / N.toDouble    //平均度数
    val singleNode = degrees.filter(_._2 == 1).count()    // 只有一个相邻节点的个数
    val nonSingleNode = degrees.filter(_._2 > 1).count()    // 相邻节点多于一个的节点个数
    var Density:Double = 0.0
    while (N != 0 )
      Density = 2*L.toDouble / (N.toDouble*(N.toDouble - 1.0))  // 网络密度 完全图的边数为n*(N-1)/2

    var cluster = 0.0   // 聚类系数
    val numerator = graph.triangleCount().vertices.map(_._2).reduce(_+_)   // triangleCount 计算一个节点的闭合三元组的个数
    val denominator = graph.inDegrees.map{
      case (_,d) => d*(d-1) / 2.0
    }.reduce(_+_)
    if(denominator != 0)
      cluster = numerator / denominator


    println("-------------图中节点个数为："+ N + "-------------" )
    println("-------------图中 边 个数为："+ L + "-------------" )
    println("-------------图的平均度数为："+ aveDegree + "-------------" )
    println("-------------图中只有一个相邻节点的个数："+ singleNode + "-------------" )
    println("-------------图中相邻节点多于一个的节点个数："+ nonSingleNode + "-------------" )
    println("-------------图的网络密度为："+ Density.formatted("%.9f") + "-------------" )
    println("-------------图的聚类系数为："+ cluster + "-------------" )
    graph.triplets.map(
      triplet =>
        triplet.srcId +"-"+ triplet.srcAttr + "|" + triplet.attr + "|" + triplet.dstId +"-"+ triplet.dstAttr
    ).take(2).foreach(println(_))
    println("-------------以上是运行LPA算法之后的新图的triplet-------------" )
    val endTime: Date=new Date()
    val time_consuming =endTime.getTime -startTime.getTime
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("mm:ss")
    val date = dateFormat.format(time_consuming)   //此为获取到的时间差
    println("-------------程序运行时间"+date)
  }

  /**
    * 计算网络密度
    * @param g
    * @tparam VD
    * @tparam ED
    * @return
    */
  def globalClusteringCoefficient[VD, ED](g:Graph[String, Int]) = {
    val numerator  = g.triangleCount().vertices.map(_._2).reduce(_ + _)
    val denominator = g.inDegrees.map{ case (_, d) => d*(d-1) / 2.0 }.reduce(_ + _)
    if(denominator == 0) 0.0 else numerator / denominator
  }

  /**
    * main Function
    * @param args
    */
  def main(args: Array[String]): Unit = {
    val startTime=new Date()
    val conf = new SparkConf()
      .setAppName("Neo4jGraphX")
//      .setMaster("local[*]")    // 本地运行参数配置，提交集群时注释掉。
      .set("spark.neo4j.bolt.url","bolt://10.10.204.24:7687")
      .set("spark.neo4j.bolt.user","neo4j")
      .set("spark.neo4j.bolt.password","kgfengjr")
    val sc = new SparkContext(conf)
    val neo = Neo4j(sc)

    //  从Neo4j中读取数据； 使用janusgraph 结合Spark进行离线计算效果更好；Neo4j+spark 可以做成异步的计算工具，支持一些简单指标的计算
//    val saneDeviceAddress = neo.cypher("match (P1:id_card) <-[real1:HasIdCard]-(income1:real_income_no) -[r:HasDeviceAddress]-> (DA1:device_address) <-[r2:HasDeviceAddress]- (income2:real_income_no)-[real2:HasIdCard] ->(P2:id_card) where id(P1) >id(P2) return distinct id(P1) as from ,id(P2) as to limit 10 ").loadRowRdd
//    val relationRdd_saneDeviceAddress:RDD[Edge[Int]] = saneDeviceAddress.map(x =>(Edge(x(0).toString.toLong,x(1).toString.toLong,1)) )
//    val defaultUser_saneDeviceAddress = (1) //  定义一个默认属性，避免有不存在用户的关系
//    // 得到Graph ：使用Graph方法，构造图garph
//    val graph_saneDeviceAddress = Graph.fromEdges(relationRdd_saneDeviceAddress,defaultUser_saneDeviceAddress)
//    getGraphMsg(graph_saneDeviceAddress)
    //    val saneDeviceIP = neo.cypher("""match (P1:id_card) 	<-[real1:HasIdCard]-(income1:real_income_no)
//                                	-[real2:HasIP]-> (other1:device_ip)
//                                    <-[real3:HasIP]- (income2:real_income_no)
//                                    -[real4:HasIdCard] ->(P2:id_card)
//                                where
//                               	id(P1) > id(P2)
//                                return
//                                	distinct id(P1) as from ,id(P2) as to """).loadRowRdd
//    val sameCompanyName = neo.cypher("""match (P1:id_card) <-[real1:HasIdCard]-(income1:real_income_no)
//                               	-[real2:HasCompanyName]-> (other1:company_name)
//                                   <-[real3:HasCompanyName]- (income2:real_income_no)
//                                   -[real4:HasIdCard] ->(P2:id_card)
//                               where
//                               	id(P1) > id(P2)
//                               return
//                               	distinct id(P1) as from ,id(P2) as to """).loadRowRdd
//    val relationRdd:RDD[Edge[Int]] = sameCompanyName.map(x =>(Edge(x(0).toString.toLong,x(1).toString.toLong,1)) )
//    val defaultUser = (1) //  定义一个默认属性，避免有不存在用户的关系
//    // 得到Graph ：使用Graph方法，构造图garph
//    val graph = Graph.fromEdges(relationRdd,defaultUser)
//    getGraphMsg(graph)
//
//    val sameAddress = neo.cypher("""match (P1:id_card)
//                                       	<-[real1:HasIdCard]-(income1:real_income_no)
//                                       	-[real2:HasHomeAddress|HasApplyAddress|HasCompanyAddress]-> (other1:address)
//                                           <-[real3:HasHomeAddress|HasApplyAddress|HasCompanyAddress]- (income2:real_income_no)
//                                           -[real4:HasIdCard] ->(P2:id_card)
//                                       where
//                                       	id(P1) >id(P2)
//                                       return
//                                       	distinct id(P1) as from ,id(P2) as to""").loadRowRdd
//    val relationRdd_sameAddress:RDD[Edge[Int]] = sameAddress.map(x =>(Edge(x(0).toString.toLong,x(1).toString.toLong,1)) )
//    val defaultUser_sameAddress = (1) //  定义一个默认属性，避免有不存在用户的关系
//    // 得到Graph ：使用Graph方法，构造图garph
//    val graph_sameAddress = Graph.fromEdges(relationRdd_sameAddress,defaultUser_sameAddress)
//    getGraphMsg(graph_sameAddress)

//    val samePhone = neo.cypher("""match (P1:id_card)
//                                       	<-[real1:HasIdCard]-(income1:real_income_no)
//                                       	-[real2:HasPhone]-> (other1:phone)
//                                           <-[real3:HasPhone]- (income2:real_income_no)
//                                           -[real4:HasIdCard] ->(P2:id_card)
//                                       where
//                                       	id(P1) >id(P2)
//                                       return
//                                       	distinct id(P1) as from ,id(P2) as to """).loadRowRdd
//    val relationRdd_samePhone:RDD[Edge[Int]] = samePhone.map(x =>(Edge(x(0).toString.toLong,x(1).toString.toLong,1)) )
//    val defaultUser_samePhone = (1) //  定义一个默认属性，避免有不存在用户的关系
//    // 得到Graph ：使用Graph方法，构造图garph
//    val graph_samePhone = Graph.fromEdges(relationRdd_samePhone,defaultUser_samePhone)
//    getGraphMsg(graph_samePhone)

//    val sameEmergencyPhone = neo.cypher("""match (P1:id_card)
//                                       	<-[real1:HasIdCard]-(income1:real_income_no)
//                                       	-[real2:HasEmergencyPhone]-> (other1:phone)
//                                           <-[real3:HasEmergencyPhone]- (income2:real_income_no)
//                                           -[real4:HasIdCard] ->(P2:id_card)
//                                       where
//                                       	id(P1) >id(P2)
//                                       return
//                                       	distinct id(P1) as from ,id(P2) as to""").loadRowRdd
//    val relationRdd_sameEmergencyPhone:RDD[Edge[Int]] = sameEmergencyPhone.map(x =>(Edge(x(0).toString.toLong,x(1).toString.toLong,1)) )
//    val defaultUser_sameEmergencyPhone = (1) //  定义一个默认属性，避免有不存在用户的关系
//    // 得到Graph ：使用Graph方法，构造图garph
//    val graph_sameEmergencyPhone = Graph.fromEdges(relationRdd_sameEmergencyPhone,defaultUser_sameEmergencyPhone)
//    getGraphMsg(graph_sameEmergencyPhone)
//
//    val samePhone_EmergencyPhone = neo.cypher("""match (P1:id_card)
//                                       	<-[real1:HasIdCard]-(income1:real_income_no)
//                                       	-[real2:HasPhone]-> (other1:phone)
//                                           <-[real3:HasEmergencyPhone]- (income2:real_income_no)
//                                           -[real4:HasIdCard] ->(P2:id_card)
//                                       where
//                                       	id(P1) <>id(P2)
//                                       return
//                                       	distinct id(P1) as from ,id(P2) as to """).loadRowRdd
//    val relationRdd_samePhone_EmergencyPhone:RDD[Edge[Int]] = samePhone_EmergencyPhone.map(x =>(Edge(x(0).toString.toLong,x(1).toString.toLong,1)) )
//    val defaultUser_samePhone_EmergencyPhone = (1) //  定义一个默认属性，避免有不存在用户的关系
//    // 得到Graph ：使用Graph方法，构造图garph
//    val graph_samePhone_EmergencyPhone = Graph.fromEdges(relationRdd_samePhone_EmergencyPhone,defaultUser_samePhone_EmergencyPhone)
//    getGraphMsg(graph_samePhone_EmergencyPhone)
//
//    val samePhone_Contact = neo.cypher("""match (P1:id_card)
//                                       <-[real1:HasIdCard]-(income1:real_income_no)
//                                      	-[real2:HasPhone]-> (other1:phone)
//                                          <-[real3:HasContact]- (income2:real_income_no)
//                                           -[real4:HasIdCard] ->(P2:id_card)
//                                      where
//                                      	id(P1) <>id(P2)
//                                       return
//                                      	distinct id(P1) as from ,id(P2) as to""").loadRowRdd
//    val relationRdd_samePhone_Contact:RDD[Edge[Int]] = samePhone_Contact.map(x =>(Edge(x(0).toString.toLong,x(1).toString.toLong,1)) )
//    val defaultUser_samePhone_Contact = (1) //  定义一个默认属性，避免有不存在用户的关系
//    // 得到Graph ：使用Graph方法，构造图garph
//    val graph_samePhone_Contact = Graph.fromEdges(relationRdd_samePhone_Contact,defaultUser_samePhone_Contact)
//    getGraphMsg(graph_samePhone_Contact)


//    val samePhone_Call = neo.cypher("""match (P1:id_card)
//                                      	<-[real1:HasIdCard]-(income1:real_income_no)
//                                      	-[real2:HasPhone]-> (other1:phone)
//                                          <-[real3:HasCall]- (income2:phone)
//                                          <-[real5:HasPhone]- (imcome3:real_income_no)
//                                          -[real4:HasIdCard] ->(P2:id_card)
//                                      where
//                                      	id(P1) <>id(P2)
//                                      return
//                                      	distinct id(P1) as from ,id(P2) as to""").loadRowRdd
//    val relationRdd_samePhone_Call:RDD[Edge[Int]] = samePhone_Call.map(x =>(Edge(x(0).toString.toLong,x(1).toString.toLong,1)) )
//    val defaultUser_samePhone_Call = (1) //  定义一个默认属性，避免有不存在用户的关系
//    // 得到Graph ：使用Graph方法，构造图garph
//    val graph_samePhone_Call = Graph.fromEdges(relationRdd_samePhone_Call,defaultUser_samePhone_Call)
//    getGraphMsg(graph_samePhone_Call)
//
//    val sameEmergencyContact = neo.cypher("""match (P1:id_card)
//                                            	<-[real1:HasIdCard]-(income1:real_income_no)
//                                            	-[real2:HasEmergencyPhone]-> (other1:phone)
//                                                <-[real3:HasContact]- (income2:real_income_no)
//                                                -[real4:HasIdCard] ->(P2:id_card)
//                                            where
//                                            	id(P1) <>id(P2)
//                                            return
//                                            	distinct id(P1) as from ,id(P2) as to""").loadRowRdd
//    val relationRdd_sameEmergencyContact:RDD[Edge[Int]] = sameEmergencyContact.map(x =>(Edge(x(0).toString.toLong,x(1).toString.toLong,1)) )
//    val defaultUser_sameEmergencyContact = (1) //  定义一个默认属性，避免有不存在用户的关系
//    // 得到Graph ：使用Graph方法，构造图garph
//    val graph_sameEmergencyContact = Graph.fromEdges(relationRdd_sameEmergencyContact,defaultUser_sameEmergencyContact)
//    getGraphMsg(graph_sameEmergencyContact)
//
//    val sameEmergencyCall = neo.cypher("""match (P1:id_card)
//                                            	<-[real1:HasIdCard]-(income1:real_income_no)
//                                            	-[real2:HasEmergencyPhone]-> (other1:phone)
//                                                <-[real3:HasCall]- (income2:phone)
//                                                <-[real5:HasPhone]- (imcome3:real_income_no)
//                                                -[real4:HasIdCard] ->(P2:id_card)
//                                            where
//                                            	id(P1) <>id(P2)
//                                            return
//                                            	distinct id(P1) as from ,id(P2) as to""").loadRowRdd
//    val relationRdd_sameEmergencyCall:RDD[Edge[Int]] = sameEmergencyCall.map(x =>(Edge(x(0).toString.toLong,x(1).toString.toLong,1)) )
//    val defaultUser_sameEmergencyCall = (1) //  定义一个默认属性，避免有不存在用户的关系
//    // 得到Graph ：使用Graph方法，构造图garph
//    val graph_sameEmergencyCall = Graph.fromEdges(relationRdd_sameEmergencyCall,defaultUser_sameEmergencyCall)
//    getGraphMsg(graph_sameEmergencyCall)

//    val ComplexSocialNet = neo.cypher("""explain
//                                        match (P1:id_card) <-[real1:HasIdCard]-(income1:real_income_no) -[r:HasDeviceAddress]-> (DA1:device_address) <-[r2:HasDeviceAddress]- (income2:real_income_no)-[real2:HasIdCard] ->(P2:id_card) where id(P1) >id(P2) return distinct id(P1) as from ,id(P2) as to
//                                        union
//                                        match (P1:id_card) 	<-[real1:HasIdCard]-(income1:real_income_no)-[real2:HasIP]-> (other1:device_ip)<-[real3:HasIP]- (income2:real_income_no)-[real4:HasIdCard] ->(P2:id_card) where	id(P1) > id(P2)return	distinct id(P1) as from ,id(P2) as to
//                                        union
//                                        match (P1:id_card) <-[real1:HasIdCard]-(income1:real_income_no)-[real2:HasCompanyName]-> (other1:company_name)<-[real3:HasCompanyName]- (income2:real_income_no)-[real4:HasIdCard] ->(P2:id_card)where	id(P1) > id(P2)return	distinct id(P1) as from ,id(P2) as to
//                                        union
//                                        match (P1:id_card) <-[real1:HasIdCard]-(income1:real_income_no)-[real2:HasHomeAddress|HasApplyAddress|HasCompanyAddress]-> (other1:address)<-[real3:HasHomeAddress|HasApplyAddress|HasCompanyAddress]- (income2:real_income_no) -[real4:HasIdCard] ->(P2:id_card)where 	id(P1) >id(P2) return distinct id(P1) as from ,id(P2) as to
//                                        union
//                                        match (P1:id_card)<-[real1:HasIdCard]-(income1:real_income_no) -[real2:HasPhone]-> (other1:phone) <-[real3:HasPhone]- (income2:real_income_no)-[real4:HasIdCard] ->(P2:id_card) where id(P1) >id(P2) return distinct id(P1) as from ,id(P2) as to
//                                        union
//                                        match (P1:id_card) <-[real1:HasIdCard]-(income1:real_income_no)-[real2:HasEmergencyPhone]-> (other1:phone)<-[real3:HasEmergencyPhone]- (income2:real_income_no) -[real4:HasIdCard] ->(P2:id_card) where	id(P1) >id(P2) return  distinct id(P1) as from ,id(P2) as to
//                                        union
//                                        match (P1:id_card)<-[real1:HasIdCard]-(income1:real_income_no) -[real2:HasPhone]-> (other1:phone)<-[real3:HasEmergencyPhone]- (income2:real_income_no) -[real4:HasIdCard] ->(P2:id_card) where 	id(P1) <>id(P2) return	distinct id(P1) as from ,id(P2) as to
//                                        union
//                                        match (P1:id_card)<-[real1:HasIdCard]-(income1:real_income_no) 	-[real2:HasPhone]-> (other1:phone)<-[real3:HasContact]- (income2:real_income_no)-[real4:HasIdCard] ->(P2:id_card)where	id(P1) <>id(P2)return 	distinct id(P1) as from ,id(P2) as to
//                                        union
//                                        match (P1:id_card) <-[real1:HasIdCard]-(income1:real_income_no) 	-[real2:HasPhone]-> (other1:phone)<-[real3:HasCall]- (income2:phone) <-[real5:HasPhone]- (imcome3:real_income_no) -[real4:HasIdCard] ->(P2:id_card) where	id(P1) <>id(P2) return	distinct id(P1) as from ,id(P2) as to
//                                        union
//                                        match (P1:id_card)	<-[real1:HasIdCard]-(income1:real_income_no)	-[real2:HasEmergencyPhone]-> (other1:phone) <-[real3:HasContact]- (income2:real_income_no) -[real4:HasIdCard] ->(P2:id_card)where	id(P1) <>id(P2)return	distinct id(P1) as from ,id(P2) as to
//                                        union
//                                        match (P1:id_card)	<-[real1:HasIdCard]-(income1:real_income_no)	-[real2:HasEmergencyPhone]-> (other1:phone) <-[real3:HasCall]- (income2:phone)<-[real5:HasPhone]- (imcome3:real_income_no) -[real4:HasIdCard] ->(P2:id_card)where	id(P1) <>id(P2)return	distinct id(P1) as from ,id(P2) as to"""
    val ComplexSocialNet = neo.cypher("match (P1:id_card) <-[real1:HasIdCard]-(income1:real_income_no) -[r:HasDeviceAddress]-> (DA1:device_address) <-[r2:HasDeviceAddress]- (income2:real_income_no)-[real2:HasIdCard] ->(P2:id_card) where id(P1) >id(P2) return distinct id(P1) as from ,id(P2) as to"
    ).loadRowRdd
    val relationRdd:RDD[Edge[Int]] = ComplexSocialNet.map(x =>(Edge(x(0).toString.toLong,x(1).toString.toLong,1)) )
    val defaultUser = (1) //  定义一个默认属性，避免有不存在用户的关系
    // 得到Graph ：使用Graph方法，构造图garph
    val graph = Graph.fromEdges(relationRdd,defaultUser)
    graph.vertices.take(1).foreach(println(_))
    val endReadNeo4j: Date = new Date()
    val time_readNeo4j = endReadNeo4j.getTime - startTime.getTime
    val dataFormat:SimpleDateFormat = new SimpleDateFormat("mm:ss")
    val readNeo4j = dataFormat.format(time_readNeo4j)
    println("-------------程序从Neo4j读取数据使用时间："+readNeo4j)
    getGraphMsg(graph)

    val nodes = graph.vertices

    val LpaGraph = Labelpropagation.run(graph,25)//lib.LabelPropagation.run(graph,10)
    // 得到LpaGraph：调用spark.graphx.lib中的LabelPropagation算法包 得到新图
    //    val LpaGraph = lib.LabelPropagation.run(graph,10)
    //    LpaGraph.vertices.top(10).foreach(println(_))
    LpaGraph.triplets.map(
      triplet =>
        triplet.srcId +"-"+ triplet.srcAttr + "|" + triplet.attr + "|" + triplet.dstId +"-"+ triplet.dstAttr
    ).top(40).foreach(println(_))
    println("-------------以上是运行LPA算法之后的新图的triplet-------------" )

    /**
      * 新的map方法
      */
    // 存储社区划分结果，（不带属性的结果）
    val LpaNodes = LpaGraph.vertices    // (5411584,5411584) 即(Vid , Cid)
    val LpaEdges = LpaGraph.edges
    val Lparesult = LpaNodes.map(x => (x._2.toLong ,x._1.toLong)).groupByKey
    Lparesult.saveAsTextFile("output_all_0927_p1")
    println("-------------存储社区划分结果，（不带属性的结果）格式为(6311410,CompactBuffer(2701784, 4451802, 6311410, 4356646, 3225776, 6032975, 4437285, 6334659, 2280217))-------------" )

    val Lpan = LpaNodes.join(nodes)   // (5411584,(5411584,(1,0))) 即 （Vid ，（Cid ，Pid））
    Lpan.top(100).foreach(println(_))
    val LpaRstFull = Lpan.map(t => (t._2._1,1)).reduceByKey(_+_)
//    LpaRstFull.take(100).foreach(println(_))
    println("-------------统计各个社区的顶点个数-------------" )
    LpaRstFull.saveAsTextFile("output_all_0927_p2")

    val end_graphx: Date = new Date()
    val time_graphX = end_graphx.getTime - endReadNeo4j.getTime
    val graphx_compute = dataFormat.format(time_graphX)
    println("-------------在当前的图数据使用GraphX计算实验结果："+graphx_compute)
    getGraphMsg(graph)


    sc.stop()
  }
}
