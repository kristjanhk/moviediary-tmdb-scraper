package eu.kyngas;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.OptionalInt;
import java.util.stream.Collectors;
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
  public void start() {
    client = vertx.createHttpClient(new HttpClientOptions().setSsl(true).setKeepAlive(false).setTrustAll(true));
    divider = config().getInteger("divider");
    divider2 = config().getInteger("divider2");

    vertx.<ArrayDeque<Integer>>executeBlocking(fut -> {
      InputStream in = MyVerticle.class.getResourceAsStream("/movie_ids_12_17_2017.json");
      ArrayDeque<Integer> result = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))
          .lines()
          .map(JsonObject::new)
          .filter(json -> json.containsKey("id"))
          .map(json -> json.getInteger("id"))
          .filter(id -> id % divider == divider2)
          .sorted()
          .collect(Collectors.toCollection(ArrayDeque::new));
      fut.complete(result);
    }, ar -> {
      if (ar.failed()) {
        log.error("Failed to read file: ", ar.cause());
        return;
      }
      moviesToFetch = ar.result();
      continueFromLastJson();
    });
  }

  private void continueFromLastJson() {
    vertx.fileSystem().readDir(System.getProperty("user.dir"), ar -> {
      if (ar.failed()) {
        log.error("Failed to read directory: ", ar.cause());
        return;
      }
      int max = 0;
      String filename = "";
      for (String s : ar.result()) {
        if (!s.contains("movies") && !s.contains(".json")) {
          continue;
        }
        String[] path = s.split("\\\\");
        int i = Integer.parseInt(path[path.length - 1].split("-")[3].split("\\.")[0]);
        if (i >= max) {
          max = i;
          filename = s;
        }
      }
      if (filename.isEmpty()) {
        log.error("Failed to find max .json file nr, starting from beginning.");
        getMovie(Retryable.create(5));
        return;
      }
      getMaxIdFromJson(filename);
    });
  }

  private void getMaxIdFromJson(String filename) {
    vertx.fileSystem().readFile(filename, ar -> {
      if (ar.failed()) {
        log.error("Failed to read max json file", ar.cause());
        return;
      }
      OptionalInt max = ar.result().toJsonObject().getJsonArray("movies").stream()
          .map(obj -> (JsonObject) obj)
          .mapToInt(json -> json.getInteger("id", -1))
          .max();
      if (!max.isPresent()) {
        log.error("Failed to find max id, starting from beginning.");
        getMovie(Retryable.create(5));
        return;
      }
      continueProgress(max.getAsInt());
    });
  }

  private void continueProgress(int fromId) {
    moviesToFetch.removeIf(id -> id <= fromId);
    log.info("Continuing after id: " + fromId);
    getMovie(Retryable.create(5));
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
    }).exceptionHandler(thr -> {
      log.error("TMDB api failure, movie id: " + moviesToFetch.poll(), thr);
      getMovie(retryable.reset());
    }).end();
  }

  private void handleOk(HttpClientResponse res, Retryable retryable) {
    Integer id = moviesToFetch.poll();
    res.bodyHandler(body -> {
      JsonObject movie = body.toJsonObject();
      movies.add(resToSave(movie));
      log.info("Added movie with id: " + id + ", response id: " + movie.getInteger("id", -1));
      getMovie(retryable.reset());
    });
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
      log.error("Rate limit reached, too many failures, trying next movie: " +
          moviesToFetch.poll() + " -> " + moviesToFetch.peek());
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
  public void stop() {
    client.close();
  }
}
