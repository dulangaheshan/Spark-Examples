//package LivyConnector
////
//}
//import org.apache.spark.api.java._
//import org.apache.spark.api.java.function._
//import org.apache.livy._
//
//import java.util
//
//
//object LivyConnector extends App with Job[Double] with Function[Int, Int] with Function2[Int, Int, Int]{
//
//  val samples: Int = 0;
//  override def call(jobContext: JobContext): Double = {
//    val list: List[Int] = List(1, 2, 3, 4)
//
//    4.0d * jobContext.sc.parallelize(list).map(u => u+1).reduce((x, y) => x max y) / samples
//  }
//
////  override def call(v1: Int): Int = 0
//  override def call(v1: Int) = {
//    val x = Math.random
//    val y = Math.random
//    if (x * x + y * y < 1) 1 else 0
//  }
////  override def call(v1: Int, v2: Int): Int = 0
//  override def call(v1: Int, v2: Int):Int =  v1 + v2

