package pro.dmitrypukhov.sparkProject1.config

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.context.annotation.{Bean, ComponentScan, Configuration}
import org.springframework.validation.beanvalidation.LocalValidatorFactoryBean
import pro.dmitrypukhov.sparkProject1.SparkEngine

/**
 * Created by dpukhov on 06.10.2016
 *
 * Main configuration class with domain beans
 * Spark initialization is moved out from SpringConfig to avoid God Class Pattern.
 */
@SpringBootApplication
@ComponentScan(Array("pro.dmitrypukhov.sparkProject1"))
class SpringConfig {

/*  @Bean
  def validator():javax.validation.Validator  = {
   new LocalValidatorFactoryBean();
}*/
  /*
  @Bean
  def sparkEngine: SparkEngine = {
    new SparkEngine
  }*/


}
