package io.cdap.cdap.runtime.spi.provisioner.remote;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class RemoteHadoopProvisionerTest {

  @Before
  public void before() {
    RemoteHadoopProvisioner.LATEST_EDGE_NODE.clear();
  }

  @Test
  public void testSelectEdgeNodeOverflow() throws InterruptedException {
    String testProfileName = "profile0";
    int testThreadsNumber = 5;
    List<String> testEdgeNodes = Arrays.asList("h0", "h1", "h2");
    RemoteHadoopConf conf = createRemoteHadoopConf(testEdgeNodes);

    Map<String, AtomicLong> edgeNodeCounters = new HashMap<>();
    for (String edgeNode : testEdgeNodes) {
      edgeNodeCounters.put(edgeNode, new AtomicLong());
    }

    List<Thread> testThreads = new ArrayList<>();
    for (int i = 0; i < testThreadsNumber; i++) {
      testThreads.add(new Thread(new ProvisionerRunner(Integer.MAX_VALUE, conf,
        testProfileName, edgeNodeCounters)));
    }
    for (Thread t : testThreads) {
      t.start();
    }
    for (Thread t : testThreads) {
      t.join();
    }
    
    Assert.assertTrue(RemoteHadoopProvisioner.LATEST_EDGE_NODE.containsKey(testProfileName));
    Assert.assertEquals(Integer.MAX_VALUE - testThreadsNumber,
      (long) RemoteHadoopProvisioner.LATEST_EDGE_NODE.get(testProfileName));

    long totalRuns = (long) Integer.MAX_VALUE * testThreadsNumber;
    long lowerBound = totalRuns / testEdgeNodes.size() - 10;
    long higherBound = totalRuns / testEdgeNodes.size() + 10;
    for (String edgeNode : testEdgeNodes) {
      long edgeNodeCalls = edgeNodeCounters.get(edgeNode).get();
      Assert.assertTrue(String.format("Checking edge node: %s with counter: %s, lower bound: %s",
          edgeNode, edgeNodeCalls, lowerBound),
        edgeNodeCalls > lowerBound);
      Assert.assertTrue(String.format("Checking edge node: %s with counter: %s, higher bound: %s",
          edgeNode, edgeNodeCalls, higherBound),
        edgeNodeCalls < higherBound);
    }
  }

  private RemoteHadoopConf createRemoteHadoopConf(List<String> testEdgeNodes) {
    Map<String, String> properties = new HashMap<>();
    properties.put(RemoteHadoopConf.HOST_PROPERTY_NAME, String.join(",", testEdgeNodes));
    properties.put(RemoteHadoopConf.USER_PROPERTY_NAME, "dummyUser");
    properties.put(RemoteHadoopConf.SSH_KEY_PROPERTY_NAME, "dummyKey");
    properties.put(RemoteHadoopConf.EDGE_NODE_CHECK_METHOD_PROPERTY_NAME, EdgeNodeCheckType.NONE.toString());

    return RemoteHadoopConf.fromProperties(properties);
  }

  private class ProvisionerRunner implements Runnable {
    private final int iterations;
    private final RemoteHadoopConf conf;
    private final String profileName;
    private final Map<String, AtomicLong> edgeNodeCounters;

    private RemoteHadoopProvisioner rhp = new RemoteHadoopProvisioner();

    public ProvisionerRunner(int iterations, RemoteHadoopConf conf, String profileName,
                             Map<String, AtomicLong> edgeNodeCounters) {
      this.iterations = iterations;
      this.conf = conf;
      this.profileName = profileName;
      this.edgeNodeCounters = edgeNodeCounters;
    }

    @Override
    public void run() {
      for (int i = 0; i < iterations; i++) {
        try {
          edgeNodeCounters.get(rhp.selectEdgeNode(conf, profileName, null)).incrementAndGet();
        } catch (NoLiveEdgeNodeException e) {
          // no op as it is impossible to get this exception by the test setting
        }
      }
    }
  }
}
