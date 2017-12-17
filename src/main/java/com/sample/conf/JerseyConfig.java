package com.sample.conf;

import org.glassfish.jersey.server.ResourceConfig;
import org.springframework.stereotype.Component;

import com.sample.resources.RestResource;

@Component
public class JerseyConfig extends ResourceConfig {

  public JerseyConfig() {
    register(RestResource.class);
  }
}
