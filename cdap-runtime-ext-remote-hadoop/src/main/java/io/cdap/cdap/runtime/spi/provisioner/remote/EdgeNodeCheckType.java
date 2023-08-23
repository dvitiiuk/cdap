package io.cdap.cdap.runtime.spi.provisioner.remote;

public enum EdgeNodeCheckType {
  PING,
  NONE;

  public static EdgeNodeCheckType fromString(String name) {
    if (name == null) {
      return NONE;
    }

    try {
      return EdgeNodeCheckType.valueOf(name.toUpperCase());
    } catch (IllegalArgumentException e) {
      return NONE;
    }
  }
}
