package eu.kyngas;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;

/**
 * @author <a href="https://github.com/kristjanhk">Kristjan Hendrik Küngas</a>
 */
public class Launcher {

  public static void main(String[] args) {
    System.setProperty("vertx.logger-delegate-factory-class-name", "io.vertx.core.logging.SLF4JLogDelegateFactory");
    Vertx.vertx()
        .deployVerticle(new MyVerticle(), new DeploymentOptions()
            .setConfig(new JsonObject()
                .put("divider", args.length == 2 ? Integer.parseInt(args[0]) : 1)
                .put("divider2", args.length == 2 ? Integer.parseInt(args[1]) : 0)));
  }
}
