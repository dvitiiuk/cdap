package io.cdap.cdap.runtime.spi.provisioner.remote;

import java.util.List;

public class NoLiveEdgeNodeException extends Exception {
  public NoLiveEdgeNodeException(LoadBalancingMethod loadBalancingMethod, List<String> hosts, int timeout) {
    super(String.format("No live node was found among: '%s' with timeout: '%s' and check method: '%s'",
      String.join(", ", hosts), timeout, loadBalancingMethod.getName()));
  }
}
