package io.cdap.cdap.runtime.spi.provisioner.remote;

import java.util.List;

public class NoLiveEdgeNodeException extends Exception {
  public NoLiveEdgeNodeException(EdgeNodeCheckType edgeNodeCheckType, List<String> hosts, int timeout) {
    super("No live node was found among: " + hosts + " with timeout: " + timeout +
      " and check method: " + edgeNodeCheckType);
  }
}
