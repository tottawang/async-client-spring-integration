package com.sample.service;

import java.util.ArrayList;
import java.util.List;

import org.springframework.integration.transformer.GenericTransformer;
import org.springframework.stereotype.Component;

import com.sample.domain.DomainObject;

@Component
public class Transformer implements GenericTransformer<String, List<DomainObject>> {

  @Override
  public List<DomainObject> transform(String group) {
    List<DomainObject> result = new ArrayList<DomainObject>();
    for (int i = 0; i < 10; i++) {
      result.add(new DomainObject("x-" + i, group));
      result.add(new DomainObject("y-" + i, group));
    }
    return result;
  }

}
