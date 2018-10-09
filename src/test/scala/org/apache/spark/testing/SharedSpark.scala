package org.apache.spark.testing

import org.apache.spark.{SparkConf, SparkUtils}
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, Suite}

trait SharedSpark extends BeforeAndAfterAll {
  self: Suite =>

  @transient private var _spark: SparkSession = _

  val sparkLogLevel:String

  implicit def spark: SparkSession = _spark

  protected implicit def reuseContextIfPossible: Boolean = false

  def appID: String = (this.getClass.getName
    + math.floor(math.random * 10E4).toLong.toString)

  def conf: SparkConf = {
    new SparkConf().
      setMaster("local[*]").
      setAppName("test").
      set("spark.ui.enabled", "false").
      set("spark.app.id", appID).
      set("spark.driver.host", "localhost")
  }

  def setup(spark: SparkSession): Unit = {
    spark.sparkContext.setLogLevel(sparkLogLevel)
    spark.sparkContext.setCheckpointDir(SparkUtils.createTempDir().toPath.toString)
  }

  override def beforeAll() {
    _spark = SparkSession.builder().config(conf).getOrCreate()
    setup(_spark)
    super.beforeAll()
  }

  override def afterAll() {
    try {
      if (!reuseContextIfPossible) {
        LocalSpark.stop(_spark)
        _spark = null
      }
    } finally {
      super.afterAll()
    }
  }

}
