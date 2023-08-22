package io.cdap.cdap.runtime.spi.provisioner.remote;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.easymock.EasyMock.anyLong;
import static org.easymock.EasyMock.anyString;
import static org.easymock.EasyMock.createStrictMock;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.partialMockBuilder;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;

public class PingEdgeNodeCheckTest {

  private static final String RUN_NODE_PING_COMMAND_METHOD_NAME = "runNodePingCommand";

  @Test
  public void testSuccessPingIndex0() throws IOException, InterruptedException, NoLiveEdgeNodeException {
    testSuccessPingWithIndex(0);
  }

  @Test
  public void testSuccessPingIndex1() throws IOException, InterruptedException, NoLiveEdgeNodeException {
    testSuccessPingWithIndex(1);
  }

  @Test
  public void testSuccessPingIndex2() throws IOException, InterruptedException, NoLiveEdgeNodeException {
    testSuccessPingWithIndex(2);
  }

  @Test
  public void testPartiallySuccessPingTimeoutIndex0() throws Exception {
    testPartiallySuccessPingTimeoutWithIndex(0);
  }

  @Test
  public void testPartiallySuccessPingTimeoutIndex1() throws Exception {
    testPartiallySuccessPingTimeoutWithIndex(1);
  }

  @Test
  public void testPartiallySuccessPingTimeoutIndex2() throws Exception {
    testPartiallySuccessPingTimeoutWithIndex(2);
  }

  @Test(expected = NoLiveEdgeNodeException.class)
  public void testNoLiveEdgeNode() throws IOException, InterruptedException, NoLiveEdgeNodeException {
    List<String> hosts = Arrays.asList("node0", "node1", "node2");

    Process pFail = createStrictMock(Process.class);
    PingEdgeNodeCheck pingEdgeNodeCheck = partialMockBuilder(PingEdgeNodeCheck.class)
      .addMockedMethod(RUN_NODE_PING_COMMAND_METHOD_NAME, String.class).createStrictMock();

    expect(pingEdgeNodeCheck.runNodePingCommand(anyString())).andReturn(pFail).times(3);
    expect(pFail.waitFor(anyLong(), eq(TimeUnit.MILLISECONDS))).andReturn(false).times(3);

    replay(pingEdgeNodeCheck, pFail);

    pingEdgeNodeCheck.selectPingableEdgeNode(hosts, 0, 100);

    verify(pingEdgeNodeCheck, pFail);
  }

  private void testSuccessPingWithIndex(int startIndex) throws IOException, InterruptedException,
    NoLiveEdgeNodeException {
    List<String> hosts = Arrays.asList("node0", "node1", "node2");

    Process p = createStrictMock(Process.class);
    PingEdgeNodeCheck pingEdgeNodeCheck = partialMockBuilder(PingEdgeNodeCheck.class)
      .addMockedMethod(RUN_NODE_PING_COMMAND_METHOD_NAME, String.class).createStrictMock();

    expect(pingEdgeNodeCheck.runNodePingCommand(anyString())).andReturn(p).anyTimes();
    expect(p.waitFor(anyLong(), eq(TimeUnit.MILLISECONDS))).andReturn(true);
    expect(p.exitValue()).andReturn(0);

    replay(pingEdgeNodeCheck, p);

    String selectedNode = pingEdgeNodeCheck.selectPingableEdgeNode(hosts, startIndex, 100);
    Assert.assertEquals(hosts.get(startIndex), selectedNode);

    verify(pingEdgeNodeCheck, p);
  }

  private void testPartiallySuccessPingTimeoutWithIndex(int startIndex) throws IOException, InterruptedException,
    NoLiveEdgeNodeException {
    List<String> hosts = Arrays.asList("node0", "node1", "node2");

    Process pFail = createStrictMock(Process.class);
    Process pSuccess = createStrictMock(Process.class);
    PingEdgeNodeCheck pingEdgeNodeCheck = partialMockBuilder(PingEdgeNodeCheck.class)
      .addMockedMethod(RUN_NODE_PING_COMMAND_METHOD_NAME, String.class).createStrictMock();

    expect(pingEdgeNodeCheck.runNodePingCommand(anyString())).andReturn(pFail);
    expect(pFail.waitFor(anyLong(), eq(TimeUnit.MILLISECONDS))).andReturn(false);

    expect(pingEdgeNodeCheck.runNodePingCommand(anyString())).andReturn(pSuccess);
    expect(pSuccess.waitFor(anyLong(), eq(TimeUnit.MILLISECONDS))).andReturn(true);
    expect(pSuccess.exitValue()).andReturn(0);

    replay(pingEdgeNodeCheck, pFail, pSuccess);

    String selectedNode = pingEdgeNodeCheck.selectPingableEdgeNode(hosts, startIndex, 100);
    Assert.assertEquals(hosts.get((startIndex + 1) % 3), selectedNode);

    verify(pingEdgeNodeCheck, pFail, pSuccess);
  }
}
