package org.radarcns.performance

import io.gatling.app.Gatling
import io.gatling.core.config.GatlingPropertiesBuilder

object SimilationRunner {
  def main(args: Array[String]) {
    val simClass = classOf[RadarPlatformSimulation].getName
    val props = new GatlingPropertiesBuilder
    props.simulationsDirectory("./src/gatling/simulations")
    props.simulationClass(simClass)
    Gatling.fromMap(props.build)
  }
}
