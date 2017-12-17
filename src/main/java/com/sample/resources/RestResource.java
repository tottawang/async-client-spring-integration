package com.sample.resources;

import java.util.HashMap;
import java.util.Map;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.integration.channel.ExecutorChannel;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.stereotype.Component;

@Component
@Produces(MediaType.APPLICATION_JSON)
@Path("/api")
public class RestResource {

  @Autowired
  @Qualifier("primaryWorkerChannel")
  ExecutorChannel primaryWorkerChannel;

  /**
   * Transfomer generates 10 items with name begin with x and y each, filter out those items begin
   * with y and then aggregate the result and do some cleanup.
   * 
   * @return
   */
  @GET
  @Path("spring-integration")
  public boolean runSpringIntegration() {
    String group = "group1";
    Map<String, Object> headers = new HashMap<String, Object>();
    return primaryWorkerChannel.send(new GenericMessage<>(group, headers));
  }

}
