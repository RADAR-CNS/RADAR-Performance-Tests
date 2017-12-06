package org.radarcns.performance

import io.gatling.app.Gatling
import io.gatling.core.config.GatlingPropertiesBuilder

object SimilationRunner {
  def main(args: Array[String]) {
    val simClass = classOf[RadarPlatformTest].getName
    val props = new GatlingPropertiesBuilder
    props.sourcesDirectory("./src/gatling/scala")
//    props.binariesDirectory("./build/classes")
    props.simulationClass(simClass)
    Gatling.fromMap(props.build)
  }
}
