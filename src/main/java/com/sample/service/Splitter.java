package com.sample.service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.integration.splitter.AbstractMessageSplitter;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;

import com.sample.domain.DomainObject;

@Component
public class Splitter extends AbstractMessageSplitter {

  private static Logger log = LoggerFactory.getLogger(Splitter.class);

  @Autowired
  private Cleaner cleaner;

  @Override
  protected Object splitMessage(Message<?> message) {
    log.info("Splitter: headers: " + message.getHeaders());

    // test aggregator memory issue with large messages
    List<DomainObject> list = new ArrayList<>();
    for (int i = 0; i < 3000; i++) {
      DomainObject object = new DomainObject("name" + i, "group1");

      char[] chars = new char[1500];
      Arrays.fill(chars, 'a');
      String str = new String(chars);
      object.setValue(str);

      list.add(object);
    }

    if (list.isEmpty()) {
      cleaner.cleanup();
    } else {
      log.info(String.format("Splitter: set count on message: %s", list.size()));
      list.forEach(m -> m.setCount(list.size()));
    }
    return list;
  }
}
