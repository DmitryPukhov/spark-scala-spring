package pro.dmitrypukhov.sparkProject1.config

import java.util._

import org.apache.spark.sql.{SparkSession, SQLContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.{Logger, LoggerFactory}
import org.springframework.context.annotation.{Bean, Configuration}
import java.util.Properties

import scala.collection.JavaConversions._

/**
 * Created by dpukhov on 30.03.16.
 *
 * Creates spark context, streaming context. This code is executed on driver once at the beginning.
 */
@Configuration
class SparkConfig {
  val LOG: Logger = LoggerFactory.getLogger(this.getClass)

  @Bean
  def createSparkConf(appConfig: AppConfig): SparkConf = {
    // Create spark configuration, init it
    val sparkConf = new SparkConf(true)
      .setAppName("Waterpump")



    // Set spark and system props
    updateSystemProperties(appConfig)
    updateSparkProperties(sparkConf, appConfig)

    sparkConf
  }

  /**
   * Update spark config properties
   */
  private def updateSparkProperties(sparkConf: SparkConf, appConfig: AppConfig) = {

    // We use third party serializers from kryo-serializers for guava immutable collections, as they are not compatible with default kryo
    //sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //sparkConf.set("spark.kryo.registrator", "com.inrix.analytics.populationDensity.engine.app.config.GuavaKryoRegistrator")

    // Load all props and push them to sparkConf
    val props: Properties = appConfig.loadAllProperties()

    props.foreach(p => {
      sparkConf.set(p._1, p._2)
    })

  }

  /**
   * Set system properties (needed for HBase ingestion)
   */
  private def updateSystemProperties(appConfig: AppConfig) = {
    val props = appConfig.loadAllProperties()
    System.setProperties(props)
  }

  /**
   * Base spark context
   * @param appConfig
   * @return
   */
  @Bean def createSparkContext(sparkConf: SparkConf, appConfig: AppConfig): SparkContext = {
    LOG.info("Creating Spark context")
    val sparkContext = new SparkContext(sparkConf)
    updateHadooopProperties(sparkContext.hadoopConfiguration, appConfig)
    sparkContext
  }

  /*  /**
     * Hbase
     * @param sparkConf
     * @param sparkContext
     * @return
     */
    @Bean
    def createHBaseContext(sparkConf: SparkConf, sparkContext: SparkContext): HBaseContext = {
      LOG.info("Creating HBase context")
      /*val hbaseConf = HBaseConfiguration.create()

      hbaseConf.addResource(sparkContext.hadoopConfiguration)*/

      val hbaseContext = new HBaseContext(sparkContext, sparkContext.hadoopConfiguration)
      hbaseContext
    }*/

  /**
   * Update properties (aws.s3. etc) for hadoop
   * @param hadoopConf
   * @param appConfig
   */
  private def updateHadooopProperties(hadoopConf: org.apache.hadoop.conf.Configuration, appConfig: AppConfig): Unit = {
    // Load all props and push them to sparkConf
    val props = appConfig.loadAllProperties()

    props.foreach(p => {
      hadoopConf.set(p._1, p._2)
    })
  }


  @Bean
  def sparkSession(sparkConf: SparkConf): SparkSession = {
    SparkSession.builder().config(sparkConf).getOrCreate()
  }

  /**
   * Spark streaming context
   * @return
   */
  @Bean
  def createStreamingContext(sparkConf: SparkContext, appConfig: AppConfig): StreamingContext = {
    LOG.info("Creating Spark Streaming context")



    // Create streaming context from sparkConif
    val batchDurationSeconds = appConfig.batchDurationSeconds
    val ssc = new StreamingContext(sparkConf, Seconds(batchDurationSeconds))

    //  Set hadoop properties
    updateHadooopProperties(ssc.sparkContext.hadoopConfiguration, appConfig)

    LOG.info("Created spark streaming context ")
    ssc
  }
}
