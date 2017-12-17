package com.sample.domain;

public class DomainObjectKey {

  private String name;
  private String group;
  private int count;

  public DomainObjectKey(String name, String group, int count) {
    this.name = name;
    this.group = group;
    this.count = count;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getGroup() {
    return group;
  }

  public void setGroup(String group) {
    this.group = group;
  }

  public int getCount() {
    return count;
  }

  public void setCount(int count) {
    this.count = count;
  }

  @Override
  public String toString() {
    return "DomainObjectKey [name=" + name + ", group=" + group + "]";
  }

}
