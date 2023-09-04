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

  private static final String GET_RUNTIME_METHOD_NAME = "getRuntime";

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
    Runtime runtime = createStrictMock(Runtime.class);
    PingEdgeNodeCheck pingEdgeNodeCheck = partialMockBuilder(PingEdgeNodeCheck.class)
      .addMockedMethod(GET_RUNTIME_METHOD_NAME).createStrictMock();

    expect(runtime.exec(anyString())).andReturn(pFail).times(3);
    expect(pingEdgeNodeCheck.getRuntime()).andReturn(runtime).times(3);
    expect(pFail.waitFor(anyLong(), eq(TimeUnit.SECONDS))).andReturn(false).times(3);

    replay(pingEdgeNodeCheck, runtime, pFail);

    pingEdgeNodeCheck.selectPingableEdgeNode(hosts, 0, 100);

    verify(pingEdgeNodeCheck, runtime, pFail);
  }

  private void testSuccessPingWithIndex(int startIndex) throws IOException, InterruptedException,
    NoLiveEdgeNodeException {
    List<String> hosts = Arrays.asList("node0", "node1", "node2");

    Process p = createStrictMock(Process.class);
    Runtime runtime = createStrictMock(Runtime.class);
    PingEdgeNodeCheck pingEdgeNodeCheck = partialMockBuilder(PingEdgeNodeCheck.class)
      .addMockedMethod(GET_RUNTIME_METHOD_NAME).createStrictMock();

    expect(runtime.exec(anyString())).andReturn(p);
    expect(pingEdgeNodeCheck.getRuntime()).andReturn(runtime);
    expect(p.waitFor(anyLong(), eq(TimeUnit.SECONDS))).andReturn(true);
    expect(p.exitValue()).andReturn(0);

    replay(pingEdgeNodeCheck, runtime, p);

    String selectedNode = pingEdgeNodeCheck.selectPingableEdgeNode(hosts, startIndex, 100);
    Assert.assertEquals(hosts.get(startIndex), selectedNode);

    verify(pingEdgeNodeCheck, runtime, p);
  }

  private void testPartiallySuccessPingTimeoutWithIndex(int startIndex) throws IOException, InterruptedException,
    NoLiveEdgeNodeException {
    List<String> hosts = Arrays.asList("node0", "node1", "node2");

    Runtime runtime = createStrictMock(Runtime.class);
    Process pFail = createStrictMock(Process.class);
    Process pSuccess = createStrictMock(Process.class);
    PingEdgeNodeCheck pingEdgeNodeCheck = partialMockBuilder(PingEdgeNodeCheck.class)
      .addMockedMethod(GET_RUNTIME_METHOD_NAME).createStrictMock();

    expect(runtime.exec(anyString())).andReturn(pFail);
    expect(pingEdgeNodeCheck.getRuntime()).andReturn(runtime).times(2);
    expect(pFail.waitFor(anyLong(), eq(TimeUnit.SECONDS))).andReturn(false);

    expect(runtime.exec(anyString())).andReturn(pSuccess);
    expect(pSuccess.waitFor(anyLong(), eq(TimeUnit.SECONDS))).andReturn(true);
    expect(pSuccess.exitValue()).andReturn(0);

    replay(pingEdgeNodeCheck, runtime, pFail, pSuccess);

    String selectedNode = pingEdgeNodeCheck.selectPingableEdgeNode(hosts, startIndex, 100);
    Assert.assertEquals(hosts.get((startIndex + 1) % 3), selectedNode);

    verify(pingEdgeNodeCheck, runtime, pFail, pSuccess);
  }
}
