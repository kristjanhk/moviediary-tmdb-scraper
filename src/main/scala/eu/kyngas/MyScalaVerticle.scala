package eu.kyngas

import java.util
import java.util.Collections

import io.vertx.core.AsyncResult
import io.vertx.core.buffer.Buffer
import io.vertx.core.json.{JsonArray, JsonObject}
import io.vertx.lang.scala.ScalaVerticle
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable
import scala.util.{Failure, Success}

/**
  * @author <a href="https://github.com/kristjanhk">Kristjan Hendrik Küngas</a>
  */
class MyScalaVerticle extends ScalaVerticle {
  val log: Logger = LoggerFactory.getLogger(classOf[MyScalaVerticle])

  override def start(): Unit = {
    log.info("Started")
    vertx.fileSystem().readDir("movies", (ar: AsyncResult[mutable.Buffer[String]]) => {
      if (ar.failed()) {
        log.error("Failed to read directory", ar.cause())
        return
      }
      vertx.executeBlocking[JsonArray](() => readFilesBlocking(ar.result())).onComplete({
        case Success(result) => writeFile(fixStructure(result))
        case Failure(cause) => log.error("Failed to read files", cause)
      })
    })
  }

  def readFilesBlocking(filePaths: mutable.Buffer[String]): JsonArray = {
    log.info("Reading files")
    val movies: JsonArray = new JsonArray()
    filePaths.foreach(path => {
      log.info("Reading file: " + path)
      movies.addAll(vertx.fileSystem().readFileBlocking(path).toJsonObject.getJsonArray("movies", new JsonArray()))
    })
    log.info("Read movies size: " + movies.size())
    movies
  }

  // TODO: 366k -> 226k, kas ei tohiks kõike ära kustutada?
  def fixStructure(movies: JsonArray): JsonArray = {
    log.info("Movies size before removing elements: " + movies.size())

    movies.getList.removeIf(x => !x.asInstanceOf[util.LinkedHashMap[String, Any]].containsKey("id"))
    movies.getList.removeIf(x => x.asInstanceOf[util.LinkedHashMap[String, Integer]].get("id") == null)
    movies.getList.removeIf(x => !x.asInstanceOf[util.LinkedHashMap[String, Any]].containsKey("title"))
    movies.getList.removeIf(x => x.asInstanceOf[util.LinkedHashMap[String, String]].get("title") == null)
    movies.getList.removeIf(x => !x.asInstanceOf[util.LinkedHashMap[String, Any]].containsKey("overview"))
    movies.getList.removeIf(x => x.asInstanceOf[util.LinkedHashMap[String, String]].get("overview") == null)
    movies.getList.removeIf(x => !x.asInstanceOf[util.LinkedHashMap[String, Any]].containsKey("genres"))
    movies.getList.removeIf(x => x.asInstanceOf[util.LinkedHashMap[String, util.ArrayList[JsonObject]]].get("genres") == null)
    movies.getList.removeIf(x => x.asInstanceOf[util.LinkedHashMap[String, util.ArrayList[JsonObject]]].get("genres").isEmpty)
    log.info("Movies size after removing elements: " + movies.size())

    Collections.sort(movies.getList.asInstanceOf[util.List[Any]], (o1: Any, o2: Any) => {
      val val1: Integer = o1.asInstanceOf[util.LinkedHashMap[String, Integer]].get("id")
      val val2: Integer = o2.asInstanceOf[util.LinkedHashMap[String, Integer]].get("id")
      val1 - val2 match {
        case x if x < 0 => -1
        case x if x > 0 => 1
        case _ => 0
      }
    })
    log.info("Sorted movies by id")

    movies
  }

  def jsonToBufferBlocking(json: JsonObject): Buffer = Buffer.buffer(json.encodePrettily())

  def writeFile(movies: JsonArray): Unit = {
    val json: JsonObject = new JsonObject().put("movies", movies)
    vertx.executeBlocking[Buffer](() => jsonToBufferBlocking(json)).onComplete({
      case Failure(cause) => log.error("Failed to convert json to buffer", cause)
      case Success(result) => vertx.fileSystem().writeFile("movies-full.json", result, ar => {
        if (ar.failed()) {
          log.error("Failed to write movies-full.json", ar.cause())
        } else {
          log.info("Wrote all movies to movies-full.json")
          vertx.close()
        }
      })

    })
  }
}
