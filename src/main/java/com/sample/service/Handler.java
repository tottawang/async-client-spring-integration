package com.sample.service;

import static org.asynchttpclient.Dsl.asyncHttpClient;
import static org.asynchttpclient.Dsl.config;

import javax.annotation.PostConstruct;

import org.asynchttpclient.AsyncHandler;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.DefaultAsyncHttpClientConfig;
import org.asynchttpclient.HttpResponseBodyPart;
import org.asynchttpclient.HttpResponseHeaders;
import org.asynchttpclient.HttpResponseStatus;
import org.asynchttpclient.Request;
import org.asynchttpclient.RequestBuilder;
import org.asynchttpclient.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.stereotype.Component;

import com.sample.domain.DomainObject;

@Component
public class Handler {

  private static Logger log = LoggerFactory.getLogger(Handler.class);
  private AsyncHttpClient client = null;

  @Autowired
  @Qualifier("aggregatorChannel")
  private DirectChannel aggregatorChannel;

  @PostConstruct
  public void init() {
    DefaultAsyncHttpClientConfig.Builder configBuilder = config();
    configBuilder.setConnectTimeout(240000).setRequestTimeout(240000).setMaxConnections(50000)
        .setMaxConnectionsPerHost(50000);
    client = asyncHttpClient(configBuilder.build());
  }

  public void getMessage(Message<?> message) {
    handleMessage(message);
  }

  public String handleMessage(Message<?> message) {
    DomainObject event = (DomainObject) message.getPayload();
    log.info("Handler: " + event.getName());
    int count = event.getCount();

    AsyncHandler<Response> asyncHandler = new AsyncHandler<Response>() {

      private final Response.ResponseBuilder builder = new Response.ResponseBuilder();

      @Override
      public void onThrowable(Throwable t) {
        log.error("on error", t);
      }

      @Override
      public State onStatusReceived(HttpResponseStatus status) throws Exception {
        log.info("on status recieved " + status.getStatusText());
        builder.accumulate(status);
        return State.ABORT;
      }

      @Override
      public State onHeadersReceived(HttpResponseHeaders headers) throws Exception {
        return State.ABORT;
      }

      @Override
      public State onBodyPartReceived(HttpResponseBodyPart bodyPart) throws Exception {
        return State.ABORT;
      }

      @Override
      public Response onCompleted() throws Exception {
        Response response = builder.build();
        log.info("send aggregator");
        aggregatorChannel.send(new GenericMessage<>(count, message.getHeaders()));
        return response;
      }
    };

    Request request = new RequestBuilder().setUrl("http://localhost:8085/api/hystrix").build();
    client.executeRequest(request, asyncHandler);
    return "";
  }

}
