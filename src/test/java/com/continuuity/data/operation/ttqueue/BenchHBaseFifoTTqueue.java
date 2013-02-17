package com.continuuity.data.operation.ttqueue;

import com.continuuity.api.data.OperationException;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data.hbase.HBaseTestBase;
import com.continuuity.data.runtime.DataFabricDistributedModule;
import com.continuuity.data.table.OVCTableHandle;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class BenchHBaseFifoTTqueue extends BenchTTQueue {

  private static Injector injector;

  private static OVCTableHandle handle;

  @BeforeClass
  public static void startEmbeddedHBase() {
    try {
      HBaseTestBase.startHBase();
      injector = Guice.createInjector(
          new DataFabricDistributedModule(HBaseTestBase.getConfiguration()));
      handle = injector.getInstance(OVCTableHandle.class);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @AfterClass
  public static void stopEmbeddedHBase() {
    try {
      HBaseTestBase.stopHBase();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected TTQueue createQueue(CConfiguration conf) throws OperationException {
    String rand = "" + Math.abs(BenchTTQueue.r.nextInt());
    return new TTQueueAbstractOnVCTable(
        handle.getTable(Bytes.toBytes("BenchTable" + rand)),
        Bytes.toBytes("BQN" + rand),
        TestTTQueue.timeOracle, conf);
  }

  // Configuration for benchmark
  private static final BenchConfig config = new BenchConfig();
  static {
    config.numJustEnqueues = 500;
    config.queueEntrySize = 10;
    config.numEnqueuesThenSyncDequeueAckFinalize = 500;
  }

  @Override
  protected BenchConfig getConfig() {
    return config;
  }

}
