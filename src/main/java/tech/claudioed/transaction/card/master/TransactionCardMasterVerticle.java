package tech.claudioed.transaction.card.master;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.cloudevents.core.message.MessageReader;
import io.cloudevents.http.vertx.VertxMessageFactory;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerRequest;

import io.vertx.core.logging.LoggerFactory;
import java.net.URI;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;

public class TransactionCardMasterVerticle extends AbstractVerticle {

  private final static Logger LOGGER = Logger.getLogger("TransactionCardMasterVerticle");

  @Override
  public void start(Promise<Void> startPromise) throws Exception {
    HttpServer server = vertx.createHttpServer();
    int port = Optional.ofNullable(System.getenv("PORT")).map(Integer::parseInt).orElse(8080);
    Optional<URI> env = Optional.ofNullable(System.getenv("SINK")).map(URI::create);
    Optional<URI> legacyServer = Optional.ofNullable(System.getenv("LEGACY_CARD")).map(URI::create);

    final var httpClient = vertx.createHttpClient();
    server.requestHandler(handleNewTransaction(httpClient, env.get(), legacyServer.get()));

    server
      // Listen and complete verticle deploy
      .listen(port, serverResult -> {
        if (serverResult.succeeded()) {
          LOGGER.info("Server started on port " + serverResult.result().actualPort());
          startPromise.complete();
        } else {
          LOGGER.log(Level.SEVERE,"Server started on port " + serverResult.result().actualPort());
          LOGGER.log(Level.SEVERE,serverResult.cause().getMessage());
          startPromise.fail(serverResult.cause());
        }
      });
  }

  public static Handler<HttpServerRequest> handleNewTransaction(HttpClient client, URI sink,
    URI legacyServer) {
    return serverRequest -> {
      // Transform the HttpRequest to Event
      VertxMessageFactory
        .createReader(serverRequest)
        .map(MessageReader::toEvent)
        .onComplete(asyncResult -> {
          if (asyncResult.succeeded()) {
            CloudEvent event = asyncResult.result();
            LOGGER.info("Received event: " + event);

            CloudEvent outputEvent = CloudEventBuilder
              .v1(event)
              .withSource(URI.create(
                "https://github.com/knative/docs/docs/serving/samples/cloudevents/cloudevents-vertx"))
              .build();

            // Prepare the http request to the sink
            HttpClientRequest sinkRequest = client.postAbs(sink.toString());

            // Define how to handle the response from the sink
            sinkRequest.handler(sinkResponse -> {
              if (sinkResponse.statusCode() >= 200 && sinkResponse.statusCode() < 300) {
                serverRequest
                  .response()
                  .setStatusCode(202)
                  .end();
              } else {
                System.out.println(
                  "Error received from sink: " + sinkResponse.statusCode() + " " + sinkResponse
                    .statusMessage());
                serverRequest
                  .response()
                  .setStatusCode(500)
                  .end();
              }
            });

            // Send the event to K_SINK
            VertxMessageFactory
              .createWriter(sinkRequest)
              .writeBinary(event);
          } else {

            LOGGER.log(Level.SEVERE,"Error while decoding the event: " + asyncResult.cause());
            // Reply with a failure
            serverRequest
              .response()
              .setStatusCode(400)
              .end();
          }
        });
    };
  }

}
