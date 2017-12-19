package eu.kyngas

import io.vertx.scala.core.Vertx

/**
  * @author <a href="https://github.com/kristjanhk">Kristjan Hendrik Küngas</a>
  */
object ScalaLauncher {

  def main(args: Array[String]): Unit = {
    System.setProperty("vertx.logger-delegate-factory-class-name", "io.vertx.core.logging.SLF4JLogDelegateFactory")
    Vertx.vertx().deployVerticle("scala:eu.kyngas.MyScalaVerticle")
  }
}
