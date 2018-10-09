package pers.wjh.bigdata.spark.rdd

import org.apache.spark.testing.SharedSpark
import org.scalatest.FunSuite


/**
  * Aggregate 测试
  *
  * spark官网 doc 说明：
  *
  * def aggregate[U](zeroValue: U)(seqOp: (U, T) ⇒ U, combOp: (U, U) ⇒ U)(implicit arg0: ClassTag[U]): U
  *
  * Aggregate the elements of each partition, and then the results for all the partitions, using given combine functions
  * and a neutral "zero value". This function can return a different result type, U, than the type of this RDD, T. Thus,
  * we need one operation for merging a T into an U and one operation for merging two U's, as in scala.TraversableOnce.
  * Both of these functions are allowed to modify and return their first argument instead of creating a new U to avoid
  * memory allocation.
  *
  * zeroValue
  * the initial value for the accumulated result of each partition for the seqOp operator, and also the initial value
  * for the combine results from different partitions for the combOp operator - this will typically be the neutral element
  * (e.g. Nil for list concatenation or 0 for summation)
  *
  * seqOp
  * an operator used to accumulate results within a partition
  *
  * combOp
  * an associative operator used to combine results from different partitions
  *
  */
class AggregateTest extends FunSuite with SharedSpark {
  override val sparkLogLevel: String = "warn"

  test("testAggregate"){
    val testRDD = spark.sparkContext.parallelize(1 to 50,5)
    val result = testRDD.aggregate(0)(
      (a, b) => {
        println(s"seqOp 过程（比较大小）： $a <=> $b")
        math.max(a, b)
      },
      (a, b) => {
        println(s"combOp 过程（最大值相加）： $a + $b")
        a + b
      }
    )
    println(s"相加结果: $result")
  }

}
