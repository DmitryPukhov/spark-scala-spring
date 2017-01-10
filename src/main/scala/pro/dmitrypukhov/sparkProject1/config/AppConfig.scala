package pro.dmitrypukhov.sparkProject1.config


import java.util.Properties

import org.slf4j.{Logger, LoggerFactory}
import org.springframework.beans.factory.annotation.{Autowired, Value}
import org.springframework.beans.factory.annotation.{Value, Autowired}
import org.springframework.core.env._
import org.springframework.stereotype.Component


import scala.collection.JavaConversions._

/**
 * Created by dpukhov on 06.09.16.
 *
 * Configuration from application.properties file
 * Properties load order:
 * 1. application.properties
 * 2. Command line overrides application.properties
 */

@Component
class AppConfig extends Serializable {
  val LOG: Logger = LoggerFactory.getLogger(this.getClass)

  /** Spring environment to load properties from */
  @Autowired
  @transient val env: Environment = null

  /**
   * Input directory
   */
  @Value("${inputUri}")
  val inputUri: String = ""

  /**
   * Output directory
   */
  @Value("${outputUri}")
  val outputUri: String = ""

  /**
   * Spark streaming batch duration
   */
  @Value("${batchDurationSeconds:600}")
  val batchDurationSeconds: Int = 0


  /**
   * Log current configuraton with properties
   */
  def logConfiguration() = {
    logPropertySources()
    logProperties()

  }

  /**
   * Write properties to log
   */
  private def logProperties() = {
    // Log properties
    val sb = new StringBuilder("Properties:\n")
    val props = loadAllProperties()
    props.toArray.sortBy(p => p._1).foreach(p => sb.append(s"${p._1}=${p._2}\n"))
    LOG.info(sb.toString())

  }

  /**
   * Get together all props
   * @return
   */
  def loadAllProperties(): Properties = {
    val props: Properties = new Properties()
    val propSources = (env.asInstanceOf[AbstractEnvironment]).getPropertySources().iterator()
    while (propSources.hasNext) {
      val propertySource = propSources.next()
      if (propertySource.isInstanceOf[MapPropertySource]) {
        propertySource.asInstanceOf[MapPropertySource]
        props.putAll(propertySource.asInstanceOf[MapPropertySource].getSource())
      }
    }
    updateFromCommandLine(props)
    props
  }

  /**
   * Update properties from command line arguments.
   * In spring command line args don't override application.properties.
   * Maybe a better way exists to configure spring.
   */
  private def updateFromCommandLine(props: Properties) = {

    val commandLineArgsName = "commandLineArgs"
    val commandLinePropertySource = this.env.asInstanceOf[AbstractEnvironment].getPropertySources.get(commandLineArgsName).asInstanceOf[SimpleCommandLinePropertySource]
    if (commandLinePropertySource != null) {
      commandLinePropertySource.getPropertyNames.foreach(name => {
        props.setProperty(name, commandLinePropertySource.getProperty(name))
      })
    } else {
      LOG.info(s"Property source ${commandLineArgsName} is empty")
    }
  }

  /**
   * List property sources to log
   *
   */
  private def logPropertySources() = {
    val sb = new StringBuilder("Property sources:\n")
    val propSources = (env.asInstanceOf[AbstractEnvironment]).getPropertySources().iterator()
    while (propSources.hasNext) {
      val propertySource = propSources.next()
      sb.append(propertySource.getName())
      sb.append("\n")
    }
    LOG.info(sb.toString())
  }

}
