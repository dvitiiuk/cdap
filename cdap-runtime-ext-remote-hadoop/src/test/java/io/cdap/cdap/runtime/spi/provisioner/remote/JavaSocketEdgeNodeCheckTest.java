package io.cdap.cdap.runtime.spi.provisioner.remote;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.easymock.EasyMock.anyInt;
import static org.easymock.EasyMock.anyString;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.partialMockBuilder;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;

public class JavaSocketEdgeNodeCheckTest {

  private static final String CHECK_NODE_METHOD_NAME = "checkNode";

  @Test
  public void testSuccessCheckIndex0() throws NoLiveEdgeNodeException {
    testSuccessCheckWithIndex(0);
  }

  @Test
  public void testSuccessCheckIndex1() throws NoLiveEdgeNodeException {
    testSuccessCheckWithIndex(1);
  }

  @Test
  public void testSuccessCheckIndex2() throws NoLiveEdgeNodeException {
    testSuccessCheckWithIndex(2);
  }

  @Test
  public void testPartiallySuccessCheckTimeoutIndex0() throws Exception {
    testPartiallySuccessCheckTimeoutWithIndex(0);
  }

  @Test
  public void testPartiallySuccessCheckTimeoutIndex1() throws Exception {
    testPartiallySuccessCheckTimeoutWithIndex(1);
  }

  @Test
  public void testPartiallySuccessCheckTimeoutIndex2() throws Exception {
    testPartiallySuccessCheckTimeoutWithIndex(2);
  }

  @Test(expected = NoLiveEdgeNodeException.class)
  public void testNoLiveEdgeNode() throws NoLiveEdgeNodeException {
    List<String> hosts = Arrays.asList("node0", "node1", "node2");

    JavaSocketEdgeNodeCheck javaSocketEdgeNodeCheck = partialMockBuilder(JavaSocketEdgeNodeCheck.class)
      .addMockedMethod(CHECK_NODE_METHOD_NAME).createStrictMock();

    expect(javaSocketEdgeNodeCheck.checkNode(anyString(), anyInt())).andReturn(false).times(3);

    replay(javaSocketEdgeNodeCheck);

    javaSocketEdgeNodeCheck.selectCheckedEdgeNode(hosts, 0, 100);

    verify(javaSocketEdgeNodeCheck);
  }

  private void testSuccessCheckWithIndex(int startIndex) throws NoLiveEdgeNodeException {
    List<String> hosts = Arrays.asList("node0", "node1", "node2");

    JavaSocketEdgeNodeCheck javaSocketEdgeNodeCheck = partialMockBuilder(JavaSocketEdgeNodeCheck.class)
      .addMockedMethod(CHECK_NODE_METHOD_NAME).createStrictMock();

    expect(javaSocketEdgeNodeCheck.checkNode(anyString(), anyInt())).andReturn(true).once();

    replay(javaSocketEdgeNodeCheck);

    String selectedNode = javaSocketEdgeNodeCheck.selectCheckedEdgeNode(hosts, startIndex, 100);
    Assert.assertEquals(hosts.get(startIndex), selectedNode);

    verify(javaSocketEdgeNodeCheck);
  }

  private void testPartiallySuccessCheckTimeoutWithIndex(int startIndex) throws NoLiveEdgeNodeException {
    List<String> hosts = Arrays.asList("node0", "node1", "node2");

    JavaSocketEdgeNodeCheck javaSocketEdgeNodeCheck = partialMockBuilder(JavaSocketEdgeNodeCheck.class)
      .addMockedMethod(CHECK_NODE_METHOD_NAME).createStrictMock();

    expect(javaSocketEdgeNodeCheck.checkNode(anyString(), anyInt())).andReturn(false).once();
    expect(javaSocketEdgeNodeCheck.checkNode(anyString(), anyInt())).andReturn(true).once();

    replay(javaSocketEdgeNodeCheck);

    String selectedNode = javaSocketEdgeNodeCheck.selectCheckedEdgeNode(hosts, startIndex, 100);
    Assert.assertEquals(hosts.get((startIndex + 1) % 3), selectedNode);

    verify(javaSocketEdgeNodeCheck);
  }
}
