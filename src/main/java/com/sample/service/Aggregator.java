package com.sample.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.integration.aggregator.ReleaseStrategy;
import org.springframework.integration.store.MessageGroup;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;

@Component
public class Aggregator implements ReleaseStrategy {

  private static Logger log = LoggerFactory.getLogger(Aggregator.class);

  @Override
  public boolean canRelease(MessageGroup group) {

    Message<?> message = group.getOne();

    // DomainObjectKey key = (DomainObjectKey) message.getPayload();
    // boolean result = group.getMessages().size() == key.getCount();

    // DomainObject event = (DomainObject) message.getPayload();
    // boolean result = group.getMessages().size() == event.getCount();

    Integer count = (Integer) message.getPayload();
    boolean result = group.getMessages().size() == count;

    if (result) {
      log.info(String.format("Aggregator: done!! %s completed %s", "", group.getMessages().size()));
    } else {
      log.info("Aggregator: not done yet!!");
    }
    return result;
  }
}
