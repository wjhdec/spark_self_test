package org.apache.spark.testing

import org.apache.spark.SparkUtils
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Suite}

trait LocalSpark extends BeforeAndAfterEach
  with BeforeAndAfterAll { self: Suite =>

  @transient var spark: SparkSession = _

  override def afterEach() {
    resetSparkContext()
    super.afterEach()
  }

  def resetSparkContext() {
    LocalSpark.stop(spark)
    spark = null
  }

}

object LocalSpark {
  def stop(spark: SparkSession) {
    Option(spark).foreach { ctx =>
      ctx.stop()
    }
    System.clearProperty("spark.driver.port")
  }

  def withSpark[T](spark: SparkSession)(f: SparkSession => T): T = {
    try {
      f(spark)
    } finally {
      stop(spark)
    }
  }

  def clearLocalRootDirs(): Unit = SparkUtils.clearLocalRootDirs()

}
