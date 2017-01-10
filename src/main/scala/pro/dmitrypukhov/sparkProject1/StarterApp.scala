package pro.dmitrypukhov.sparkProject1


import pro.dmitrypukhov.sparkProject1.config.SpringConfig
import org.apache.spark.SparkContext
import org.slf4j.{LoggerFactory, Logger}
import org.springframework.boot.SpringApplication


/**
 * Application entry point with main() function
 * Parameters from args will overwrite those from application.properties
 *
 */
object StarterApp{
  val LOG: Logger = LoggerFactory.getLogger(this.getClass)

  /**
   * Initializing Spring and start Spark app
   * @param args
   */
  def main(args: Array[String]) {
    LOG.info("----------------------------------------------------------------------------------------------------------------")
    LOG.info("----------------       Spark engine v. 0.0.1 ")
    LOG.info("----------------------------------------------------------------------------------------------------------------")

    // Create Spring context
    val ctx = SpringApplication.run(Array[Object]{classOf[SpringConfig]}, args)
    val engine = ctx.getBean(classOf[SparkEngine])

    sys addShutdownHook {
      LOG.info("Shutdown hook called")
      engine.stop()
      LOG.info("Engine stopped gracefully")
    }

    // Start the engine
    engine.start()
  }

  def start(): Unit = {

  }
}
