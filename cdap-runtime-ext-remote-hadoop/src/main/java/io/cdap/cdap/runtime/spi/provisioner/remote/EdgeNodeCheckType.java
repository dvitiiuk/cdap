package io.cdap.cdap.runtime.spi.provisioner.remote;

public enum EdgeNodeCheckType {
  PING("ping"),
  NONE("none");

  private final String name;

  EdgeNodeCheckType(String name) {
    this.name = name;
  }

  public static EdgeNodeCheckType getEdgeNodeCheck(String name) {
    if (name == null) {
      return NONE;
    }
    for (EdgeNodeCheckType enc : EdgeNodeCheckType.values()) {
      if (enc.name.equalsIgnoreCase(name)) {
        return enc;
      }
    }
    return NONE;
  }
}
