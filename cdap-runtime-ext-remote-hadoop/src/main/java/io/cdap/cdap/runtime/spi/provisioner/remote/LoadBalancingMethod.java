package io.cdap.cdap.runtime.spi.provisioner.remote;

public enum LoadBalancingMethod {
  ROUND_ROBIN("roundRobin"),
  ROUND_ROBIN_PING("roundRobinPing");

  private final String name;

  LoadBalancingMethod(String name) {
    this.name = name;
  }

  public String getName() {
    return name;
  }

  public static LoadBalancingMethod fromString(String name) {
    if (name == null) {
      return ROUND_ROBIN;
    }
    for (LoadBalancingMethod lbm : LoadBalancingMethod.values()) {
      if (lbm.name.equalsIgnoreCase(name)) {
        return lbm;
      }
    }
    return ROUND_ROBIN;
  }
}
