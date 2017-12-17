package eu.kyngas;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import java.util.ArrayDeque;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;

/**
 * @author <a href="https://github.com/kristjanhk">Kristjan Hendrik Küngas</a>
 */
@Slf4j
public class MyVerticle extends AbstractVerticle {
  private HttpClient client;
  private JsonArray movies = new JsonArray();
  private ArrayDeque<Integer> moviesToFetch;
  private int counter = 0;
  private Integer divider;
  private Integer divider2;

  @Override
  public void start() throws Exception {
    client = vertx.createHttpClient(new HttpClientOptions().setSsl(true).setKeepAlive(false).setTrustAll(true));
    divider = config().getInteger("divider");
    divider2 = config().getInteger("divider2");

    vertx.fileSystem().readFile("movie_ids_12_17_2017.json", ar -> {
      if (ar.failed()) {
        log.error("Failed to read file: ", ar.cause());
      }
      String result = ar.result().toString();
      moviesToFetch = Stream.of(result.split("\n"))
          .map(JsonObject::new)
          .filter(json -> json.containsKey("id"))
          .map(json -> json.getInteger("id"))
          .filter(id -> id % divider == divider2)
          .sorted()
          .collect(Collectors.toCollection(ArrayDeque::new)); //võtab tükk aega
      getMovie(Retryable.create(5));
    });
  }

  private void getMovie(Retryable retryable) {
    if (moviesToFetch.size() % 100 == 0 || moviesToFetch.isEmpty()) {
      JsonArray currentMovies = movies.copy();
      movies.clear();
      int c = counter++;
      String filename = "movies-" + divider + "-" + divider2 + "-" + c + ".json";
      vertx.fileSystem().writeFile(filename, Buffer.buffer(new JsonObject().put("movies", currentMovies).encodePrettily()), ar -> {
        if (ar.failed()) {
          log.error("Failed to write to file " + filename + ": ", ar.cause());
          return;
        }
        log.info("Wrote movies to " + filename);
        if (moviesToFetch.isEmpty()) {
          vertx.close();
        }
      });
      if (moviesToFetch.isEmpty()) {
        return;
      }
    }
    String uri = "/3/movie/" + moviesToFetch.peek() + "?api_key=fbe0eec213cc4dd1dbb4a8c222273a3e";
    client.get(443, "api.themoviedb.org", uri, res -> {
      if (res.statusCode() == 200) {
        handleOk(res, retryable);
      } else if (res.statusCode() == 429) {
        handleRateLimit(res, retryable);
      } else if (res.statusCode() == 404) {
        handleNotFound(res, retryable);
      } else {
        handleElse(res, retryable);
      }
    }).end();
  }

  private void handleOk(HttpClientResponse res, Retryable retryable) {
    res.bodyHandler(body -> {
      JsonObject movie = body.toJsonObject();
      movies.add(resToSave(movie));
      log.info("Added movie with id: " + movie.getInteger("id", -1));
    });
    moviesToFetch.poll();
    getMovie(retryable.reset());
  }

  private JsonObject resToSave(JsonObject movie) {
    JsonObject toBeInserted = new JsonObject();
    toBeInserted.put("id", movie.getInteger("id", -1));
    toBeInserted.put("title", movie.getString("title", "null"));
    toBeInserted.put("overview", movie.getString("overview", "null"));
    toBeInserted.put("genres", movie.getJsonArray("genres", new JsonArray()));
    return toBeInserted;
  }

  private void handleRateLimit(HttpClientResponse res, Retryable retryable) {
    retryable.retry(() -> {
      long timeTillReset = getTimeTillReset(res);
      log.warn("Rate limit reached, waiting for " + timeTillReset + " ms.");
      vertx.setTimer(timeTillReset, timer -> getMovie(retryable));
    }, () -> {
      log.error("Rate limit reached, too many failures, trying next movie: " + moviesToFetch.poll() + " -> " + moviesToFetch.peek());
      getMovie(retryable.reset());
    });
  }

  private void handleNotFound(HttpClientResponse res, Retryable retryable) {
    log.info("Missing movie with id: " + moviesToFetch.poll());
    getMovie(retryable.reset());
  }

  private void handleElse(HttpClientResponse res, Retryable retryable) {
    log.error("TMDB api failure, movie id: " + moviesToFetch.poll() +
        ": statuscode: " + res.statusCode() +
        "; status message: " + res.statusMessage());
    getMovie(retryable.reset());
  }

  private long getTimeTillReset(HttpClientResponse res) {
    return Long.parseLong(res.getHeader("X-RateLimit-Reset")) - System.currentTimeMillis() + 500L;
  }

  @Override
  public void stop() throws Exception {
    client.close();
  }
}
