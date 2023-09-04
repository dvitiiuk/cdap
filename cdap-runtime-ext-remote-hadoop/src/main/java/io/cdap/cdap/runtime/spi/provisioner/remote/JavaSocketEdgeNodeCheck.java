package io.cdap.cdap.runtime.spi.provisioner.remote;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.List;

public class JavaSocketEdgeNodeCheck {
  private static final Logger LOG = LoggerFactory.getLogger(JavaSocketEdgeNodeCheck.class);
  private static final int CHECK_PORT = 22;

  public String selectCheckedEdgeNode(List<String> hosts, int initialIndex, int timeout)
    throws NoLiveEdgeNodeException {
    for (int attemptsNumber = 0; attemptsNumber < hosts.size(); attemptsNumber++) {
      String hostToCheck = hosts.get((initialIndex + attemptsNumber) % hosts.size());
      if (checkNode(hostToCheck, timeout)) {
        return hostToCheck;
      }
    }
    throw new NoLiveEdgeNodeException(LoadBalancingMethod.ROUND_ROBIN_SOCKET, hosts, timeout);
  }

  boolean checkNode(String hostToCheck, int timeout) {
    try (Socket s = new Socket()) {
      s.connect(new InetSocketAddress(hostToCheck, CHECK_PORT), timeout);

      LOG.debug(String.format("Edge node '%s' check passed for timeout '%d'.", hostToCheck, timeout));
      return true;
    } catch (SocketTimeoutException e) {
      LOG.warn(String.format("Edge node '%s' check timeout after '%d' milliseconds. Message: %s",
        hostToCheck, timeout, e.getMessage()));
    } catch (Exception e) {
      // print full stacktrace as there can be numerous reasons of failure
      LOG.warn(String.format("Edge node '%s' check failed with exception.", hostToCheck), e);
    }
    return false;
  }
}
