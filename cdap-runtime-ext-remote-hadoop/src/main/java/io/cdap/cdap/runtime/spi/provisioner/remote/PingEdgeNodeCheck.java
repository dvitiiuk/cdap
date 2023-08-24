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
    for (int attemptsNumber = 0; attemptsNumber < hosts.size(); attemptsNumber++) {
      String hostToCheck = hosts.get((initialIndex + attemptsNumber) % hosts.size());
      if (pingNode(hostToCheck, timeout)) {
        return hostToCheck;
      }
    }
    throw new NoLiveEdgeNodeException(EdgeNodeCheckType.PING, hosts, timeout);
  }

  boolean pingNode(String hostToCheck, int timeout) {
    try {
      // we need guarantee that process won't hang and kill it if timeout is significantly exceeded
      int processTimeout = timeout * 2;

      Process p = getRuntime().exec(String.format("ping -n -c 1 -w %d %s", timeout, hostToCheck));
      boolean waitResult = p.waitFor(processTimeout, TimeUnit.SECONDS);

      if (!waitResult) {
        logTimeoutPing(hostToCheck, processTimeout, p);
        return false;
      }

      int ec = p.exitValue();
      if (ec > 0) {
        logFailedPing(hostToCheck, ec, p);
        return false;
      }

      logSuccessPing(hostToCheck, p);
      return true;
    } catch (IOException | InterruptedException e) {
      LOG.error(e.getMessage(), e);
      return false;
    }
  }

  Runtime getRuntime () {
    return Runtime.getRuntime();
  }

  private static String getProcessOutput(Process p) {
    return new BufferedReader(new InputStreamReader(p.getInputStream())).lines().collect(Collectors.joining("\n"));
  }

  private static String getProcessError(Process p) {
    return new BufferedReader(new InputStreamReader(p.getErrorStream())).lines().collect(Collectors.joining("\n"));
  }

  private static void logSuccessPing(String hostToCheck, Process p) {
    if (LOG.isDebugEnabled()) {
      LOG.debug(String.format("Edge node '%s' check passed. Error: %s. Output: %s",
        hostToCheck, getProcessError(p), getProcessOutput(p)));
    }
  }

  private static void logFailedPing(String hostToCheck, int exitCode, Process p) {
    if (LOG.isWarnEnabled()) {
      LOG.warn(String.format("Edge node '%s' check failed with exit code %d. Error: %s. Output: %s",
        hostToCheck, exitCode, getProcessError(p), getProcessOutput(p)));
    }
  }

  private static void logTimeoutPing(String hostToCheck, int processTimeout, Process p) {
    if (LOG.isWarnEnabled()) {
      LOG.warn(String.format("Edge node '%s' check process timeout after %d seconds. Error: %s. Output: %s",
        hostToCheck, processTimeout, getProcessError(p), getProcessOutput(p)));
    }
  }
}
