package com.sample.service;

import org.springframework.integration.core.GenericSelector;
import org.springframework.stereotype.Component;

import com.sample.domain.DomainObject;

@Component
public class Filter implements GenericSelector<DomainObject> {

  @Override
  public boolean accept(DomainObject domainObject) {
    return domainObject.getName().startsWith("x");
  }
}
