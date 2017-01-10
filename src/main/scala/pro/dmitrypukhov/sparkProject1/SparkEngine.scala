package pro.dmitrypukhov.sparkProject1


import javax.inject.Inject

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext
import org.slf4j.{Logger, LoggerFactory}
import org.springframework.stereotype.Component
import pro.dmitrypukhov.sparkProject1.config.AppConfig

/**
 * Created by dima on 10/6/16.
 *
 * Main Spark application
 *
 */
@Component
class     SparkEngine {
  private val LOG: Logger = LoggerFactory.getLogger(this.getClass)

  @Inject  @transient
  private val sparkContext: SparkContext = null

  @Inject  @transient
  private val streamingContext: StreamingContext = null

  @Inject  @transient
  private val sparkSession: SparkSession = null

  @Inject
  private val appConfig: AppConfig = null

  /**
   * Spark engine entry point
   */
  def start() = {

    // Read from input csv
    LOG.info(s"Reading from ${appConfig.inputUri}")
    val inputDf = sparkSession.read.options(
      Map("header" -> "true"
      )).csv(appConfig.inputUri)

    // Display schema and 10 first records
    inputDf.printSchema()
    inputDf.show(10)

  }

  /**
   * Stop streaming at the end
   */
  def stop(): Unit = {
    streamingContext.stop(stopSparkContext = true, stopGracefully = true)
  }


}
