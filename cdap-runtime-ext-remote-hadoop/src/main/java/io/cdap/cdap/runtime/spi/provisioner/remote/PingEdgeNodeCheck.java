package io.cdap.cdap.runtime.spi.provisioner.remote;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class PingEdgeNodeCheck {
  private static final Logger LOG = LoggerFactory.getLogger(PingEdgeNodeCheck.class);

  public String selectPingableEdgeNode(List<String> hosts, int initialIndex, int timeout)
    throws NoLiveEdgeNodeException {
    int attemptsNumber = 0;
    while (attemptsNumber < hosts.size()) {
      String hostToCheck = hosts.get((initialIndex + attemptsNumber) % hosts.size());
      String checkedHost = runNodePingCheck(hostToCheck, timeout);
      if (checkedHost != null) {
        return checkedHost;
      }
      attemptsNumber++;
    }
    throw new NoLiveEdgeNodeException(EdgeNodeCheckType.PING, hosts, timeout);
  }

  String runNodePingCheck(String hostToCheck, int timeout) {
    try {
      Process p = runNodePingCommand(hostToCheck);
      boolean waitResult = p.waitFor(timeout, TimeUnit.MILLISECONDS);
      if (waitResult) {
        int ec = p.exitValue();
        if (ec == 0) {
          if (LOG.isTraceEnabled()) {
            LOG.trace("Edge node '" + hostToCheck + "' check passed. Error: " +
              getProcessError(p) + ". Output: " + getProcessOutput(p));
          }
          return hostToCheck;
        } else {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Edge node '" + hostToCheck + "' check failed with exit code " + ec + ". Error: " +
              getProcessError(p) + ". Output: " + getProcessOutput(p));
          }
        }
      } else {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Edge node '" + hostToCheck + "' check timeout after " + timeout + " seconds. Error: " +
            getProcessError(p) + ". Output: " + getProcessOutput(p));
        }
      }
    } catch (IOException | InterruptedException e) {
      LOG.error(e.getMessage(), e);
    }
    return null;
  }

  Process runNodePingCommand(String hostToCheck) throws IOException {
    return Runtime.getRuntime().exec("ping -c 1 " + hostToCheck);
  }

  private static String getProcessOutput(Process p) {
    return new BufferedReader(new InputStreamReader(p.getInputStream())).lines()
      .collect(Collectors.joining("\n"));
  }

  private static String getProcessError(Process p) {
    return new BufferedReader(new InputStreamReader(p.getErrorStream())).lines()
      .collect(Collectors.joining("\n"));
  }
}
