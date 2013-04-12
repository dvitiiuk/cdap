package com.continuuity.data.operation.executor.remote;

import com.continuuity.api.data.OperationException;
import com.continuuity.data.operation.ClearFabric;
import com.continuuity.data.operation.CompareAndSwap;
import com.continuuity.data.operation.Delete;
import com.continuuity.data.operation.Increment;
import com.continuuity.data.operation.Operation;
import com.continuuity.data.operation.OperationContext;
import com.continuuity.data.operation.Read;
import com.continuuity.data.operation.ReadAllKeys;
import com.continuuity.data.operation.ReadColumnRange;
import com.continuuity.data.operation.Write;
import com.continuuity.data.operation.WriteOperation;
import com.continuuity.data.operation.ttqueue.DequeueResult;
import com.continuuity.data.operation.ttqueue.QueueAck;
import com.continuuity.data.operation.ttqueue.QueueAdmin;
import com.continuuity.data.operation.ttqueue.QueueConfig;
import com.continuuity.data.operation.ttqueue.QueueConsumer;
import com.continuuity.data.operation.ttqueue.QueueDequeue;
import com.continuuity.data.operation.ttqueue.QueueEnqueue;
import com.continuuity.data.operation.ttqueue.QueueEntry;
import com.continuuity.data.operation.ttqueue.QueueEntryPointer;
import com.continuuity.data.operation.ttqueue.QueuePartitioner.PartitionerType;
import com.continuuity.data.operation.ttqueue.StatefulQueueConsumer;
import com.continuuity.data.util.OperationUtil;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;
import org.mortbay.log.Log;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static com.continuuity.data.operation.ttqueue.QueueAdmin.GetQueueInfo;
import static com.continuuity.data.operation.ttqueue.QueueAdmin.QueueInfo;

public abstract class OperationExecutorServiceTest extends
    OpexServiceTestBase {

  static OperationContext context = OperationUtil.DEFAULT;

  /** Tests Write, Read */
  @Test
  public void testWriteThenRead() throws Exception {
    final byte[] key1 = "tWTRkey1".getBytes();
    final byte[] key2 = "tWTRkey2".getBytes();

    // write one column with remote
    Write write = new Write(key1, "col1".getBytes(), "val1".getBytes());
    remote.commit(context, write);
    // read back with remote and compare
    Read read = new Read(key1, "col1".getBytes());
    Map<byte[], byte[]> columns = remote.execute(context, read).getValue();
    Assert.assertEquals(1, columns.size());
    Assert.assertArrayEquals("val1".getBytes(), columns.get("col1".getBytes()));

    // write two columns with remote
    write = new Write(key2,
        new byte[][] { "col2".getBytes(), "col3".getBytes() },
        new byte[][] { "val2".getBytes(), "val3".getBytes() });
    remote.commit(context, write);
    // read back with remote and compare
    read = new Read(key2,
        new byte[][] { "col2".getBytes(), "col3".getBytes() });
    columns = remote.execute(context, read).getValue();
    Assert.assertEquals(2, columns.size());
    Assert.assertArrayEquals("val2".getBytes(), columns.get("col2".getBytes()));
    Assert.assertArrayEquals("val3".getBytes(), columns.get("col3".getBytes()));
  }

  /** Tests Increment, Read */
  @Test
  public void testIncrementThenRead() throws Exception {
    final byte[] count = "tITRcount".getBytes();
    final byte[] col = { 'c', 'o', 'l' };

    // increment one column with remote
    Increment increment = new Increment(count, col, 1);
    remote.increment(context, increment);
    // read back with remote and verify it is 1
    Read read = new Read(count, col);
    Map<byte[], byte[]> result = remote.execute(context, read).getValue();
    Assert.assertNotNull(result);
    byte[] value = result.get(col);
    Assert.assertEquals(8, value.length);
    Assert.assertEquals(1L, ByteBuffer.wrap(value).asLongBuffer().get());

    // increment two columns with remote
    increment = new Increment(count,
        new byte[][] { "a".getBytes(), col },
        new long[] { 5L, 10L } );
    remote.increment(context, increment);
    // read back with remote and verify values
    read = new Read(count,
        new byte[][] { "a".getBytes(), col });
    Map<byte[], byte[]> columns = remote.execute(context, read).getValue();
    Assert.assertNotNull(columns);
    Assert.assertEquals(2, columns.size());
    Assert.assertEquals(5L,
        ByteBuffer.wrap(columns.get("a".getBytes())).asLongBuffer().get());
    Assert.assertEquals(11L,
        ByteBuffer.wrap(columns.get(col)).asLongBuffer().get());
  }

  /** Tests read for non-existent key */
  @Test
  public void testDeleteThenRead() throws Exception {

    // this is the key we will use
    final byte[] key = "tDTRkey".getBytes();
    final byte[] col = "col".getBytes();

    // write a key/value
    Write write = new Write(key, col, "here".getBytes());
    remote.commit(context, write);

    // delete the row with remote
    Delete delete = new Delete(key, col);
    remote.commit(context, delete);

    // read back key with remote and verify null
    Read read = new Read(key, col);
    Assert.assertTrue(remote.execute(context, read).isEmpty());

    // read back one column and verify null
    read = new Read(key, "none".getBytes());
    Assert.assertTrue(remote.execute(context, read).isEmpty());

    // read back two columns and verify null
    read = new Read(key,
        new byte[][] { "neither".getBytes(), "nor".getBytes() });
    Assert.assertTrue(remote.execute(context, read).isEmpty());

    // read back column range and verify null
    ReadColumnRange readColumnRange = new ReadColumnRange(
        key,
        "from".getBytes(),
        "to".getBytes());
    Assert.assertTrue(remote.execute(context, readColumnRange).isEmpty());
  }

   /** Tests Write, ReadColumnRange, Delete */
  @Test
  public void testWriteThenRangeThenDelete() throws Exception {

    final byte[] row = "tWTRTDrow".getBytes();

    // write a bunch of columns with remote
    Write write = new Write(row,
        new byte[][] { "a".getBytes(), "b".getBytes(), "c".getBytes() },
        new byte[][] { "1".getBytes(), "2".getBytes(), "3".getBytes() });
    remote.commit(context, write);

    // read back all columns with remote (from "" ... "")
    ReadColumnRange readColumnRange =
        new ReadColumnRange(row, null, null);
    Map<byte[], byte[]> columns =
        remote.execute(context, readColumnRange).getValue();
    // verify it is complete
    Assert.assertNotNull(columns);
    Assert.assertEquals(3, columns.size());
    Assert.assertArrayEquals("1".getBytes(), columns.get("a".getBytes()));
    Assert.assertArrayEquals("2".getBytes(), columns.get("b".getBytes()));
    Assert.assertArrayEquals("3".getBytes(), columns.get("c".getBytes()));

    // read back all columns with remote (from "" ... "")
    readColumnRange =
        new ReadColumnRange(row, null, null, 1);
    columns =
        remote.execute(context, readColumnRange).getValue();
    // verify it is complete
    Assert.assertNotNull(columns);
    Assert.assertEquals(1, columns.size());
    Assert.assertArrayEquals("1".getBytes(), columns.get("a".getBytes()));

    // read back a sub-range (from aa to bb, should only return b)
    readColumnRange =
        new ReadColumnRange(row, "aa".getBytes(), "bb".getBytes());
    columns = remote.execute(context, readColumnRange).getValue();
    Assert.assertNotNull(columns);
    Assert.assertEquals(1, columns.size());
    Assert.assertNull(columns.get("a".getBytes()));
    Assert.assertArrayEquals("2".getBytes(), columns.get("b".getBytes()));
    Assert.assertNull(columns.get("c".getBytes()));

    // read back all columns after aa, should return b and c
    readColumnRange =
        new ReadColumnRange(row, "aa".getBytes(), null);
    columns = remote.execute(context, readColumnRange).getValue();
    Assert.assertNotNull(columns);
    Assert.assertEquals(2, columns.size());
    Assert.assertNull(columns.get("a".getBytes()));
    Assert.assertArrayEquals("2".getBytes(), columns.get("b".getBytes()));
    Assert.assertArrayEquals("3".getBytes(), columns.get("c".getBytes()));

    // read back all columns before bb, should return a and b
    readColumnRange =
        new ReadColumnRange(row, null, "bb".getBytes());
    columns = remote.execute(context, readColumnRange).getValue();
    Assert.assertNotNull(columns);
    Assert.assertEquals(2, columns.size());
    Assert.assertArrayEquals("1".getBytes(), columns.get("a".getBytes()));
    Assert.assertArrayEquals("2".getBytes(), columns.get("b".getBytes()));
    Assert.assertNull(columns.get("c".getBytes()));

    // read back a disjoint column range, verify it is empty by not null
    readColumnRange =
        new ReadColumnRange(row, "d".getBytes(), "e".getBytes());
    Assert.assertTrue(remote.execute(context, readColumnRange).isEmpty());

    // delete two of the columns with remote
    Delete delete = new Delete(row,
        new byte[][] { "a".getBytes(), "c".getBytes() });
    remote.commit(context, delete);

    // read back the column range again with remote
    readColumnRange = // reads everything
        new ReadColumnRange(row, null, null);
    columns = remote.execute(context, readColumnRange).getValue();
    Assert.assertNotNull(columns);
    // verify the two are gone
    Assert.assertEquals(1, columns.size());
    Assert.assertNull(columns.get("a".getBytes()));
    Assert.assertArrayEquals("2".getBytes(), columns.get("b".getBytes()));
    Assert.assertNull(columns.get("c".getBytes()));
  }

  /** Tests Write, CompareAndSwap, Read */
  @Test
  public void testWriteThenSwapThenRead() throws Exception {

    final byte[] key = "tWTSTRkey".getBytes();

    // write a column with a value
    Write write = new Write(key, "x".getBytes(), "1".getBytes());
    remote.commit(context, write);

    // compareAndSwap with actual value
    CompareAndSwap compareAndSwap = new CompareAndSwap(key,
        "x".getBytes(), "1".getBytes(), "2".getBytes());
    remote.commit(context, compareAndSwap);

    // read back value and verify it swapped
    Read read = new Read(key, "x".getBytes());
    Map<byte[], byte[]> columns = remote.execute(context, read).getValue();
    Assert.assertNotNull(columns);
    Assert.assertEquals(1, columns.size());
    Assert.assertArrayEquals("2".getBytes(), columns.get("x".getBytes()));

    // compareAndSwap with different value
    compareAndSwap = new CompareAndSwap(key,
        "x".getBytes(), "1".getBytes(), "3".getBytes());
    try {
      remote.commit(context, compareAndSwap);
      Assert.fail("Expected compare-and-swap to fail.");
    } catch (OperationException e) {
      //expected
    }

    // read back and verify it has not swapped
    columns = remote.execute(context, read).getValue();
    Assert.assertNotNull(columns);
    Assert.assertEquals(1, columns.size());
    Assert.assertArrayEquals("2".getBytes(), columns.get("x".getBytes()));

    // delete the row
    Delete delete = new Delete(key, "x".getBytes());
    remote.commit(context, delete);

    // verify the row is not there any more, actually the read will return
    // a map with an entry for x, but with a null value
    Assert.assertTrue(remote.execute(context, read).isEmpty());

    // compareAndSwap
    compareAndSwap = new CompareAndSwap(key,
        "x".getBytes(), "2".getBytes(), "3".getBytes());
    try {
      remote.commit(context, compareAndSwap);
      Assert.fail("Expected compare-and-swap to fail.");
    } catch (OperationException e) {
      //expected
    }

    // verify the row is still not there
    Assert.assertTrue(remote.execute(context, read).isEmpty());
  }

  /** clear the tables, then write a batch of keys, then readAllKeys */
  @Test
  public void testWriteBatchThenReadAllKeys() throws Exception  {
    // clear all data, otherwise we will get keys from other tests
    // mingled into the responses for ReadAllKeys
    remote.execute(context, new ClearFabric(ClearFabric.ToClear.DATA));

    // list all keys, verify it is empty (@Before clears the data fabric)
    ReadAllKeys readAllKeys = new ReadAllKeys(0, 1);
    List<byte[]> keys = remote.execute(context, readAllKeys).getValue();
    Assert.assertNotNull(keys);
    Assert.assertEquals(0, keys.size());
    // write a batch, some k/v, some single column, some multi-column
    List<WriteOperation> writes = Lists.newArrayList();
    writes.add(new Write("a".getBytes(), "c".getBytes(), "1".getBytes()));
    writes.add(new Write("b".getBytes(), "c".getBytes(), "2".getBytes()));
    writes.add(new Write("c".getBytes(), "c".getBytes(), "3".getBytes()));
    writes.add(new Write("d".getBytes(), "x".getBytes(), "4".getBytes()));
    writes.add(new Write("e".getBytes(), "y".getBytes(), "5".getBytes()));
    writes.add(new Write("f".getBytes(), "z".getBytes(), "6".getBytes()));
    writes.add(new Write("g".getBytes(), new byte[][] { "x".getBytes(), "y".getBytes(), "z".getBytes() },
                                         new byte[][] { "7".getBytes(), "8".getBytes(), "9".getBytes() }));
    remote.commit(context, writes);

    // readAllKeys with > number of writes
    readAllKeys = new ReadAllKeys(0, 10);
    keys = remote.execute(context, readAllKeys).getValue();
    Assert.assertNotNull(keys);
    Assert.assertEquals(7, keys.size());

    // readAllKeys with < number of writes
    readAllKeys = new ReadAllKeys(0, 5);
    keys = remote.execute(context, readAllKeys).getValue();
    Assert.assertNotNull(keys);
    Assert.assertEquals(5, keys.size());

    // readAllKeys with offset and returning all
    readAllKeys = new ReadAllKeys(4, 4);
    keys = remote.execute(context, readAllKeys).getValue();
    Assert.assertNotNull(keys);
    Assert.assertEquals(3, keys.size());

    // readAllKeys with offset not returning all
    readAllKeys = new ReadAllKeys(2, 4);
    keys = remote.execute(context, readAllKeys).getValue();
    Assert.assertNotNull(keys);
    Assert.assertEquals(4, keys.size());

    // readAllKeys with offset returning none
    readAllKeys = new ReadAllKeys(7, 5);
    keys = remote.execute(context, readAllKeys).getValue();
    Assert.assertNotNull(keys);
    Assert.assertEquals(0, keys.size());
  }

  static final byte[] kvcol = Operation.KV_COL;

  /** test batch, one that succeeds and one that fails */
  @Test
  public void testBatchSuccessAndFailure() throws Exception {

    final byte[] keyA = "tBSAF.a".getBytes();
    final byte[] keyB = "tBSAF.b".getBytes();
    final byte[] keyC = "tBSAF.c".getBytes();
    final byte[] keyD = "tBSAF.d".getBytes();
    final byte[] q = "tBSAF.q".getBytes();
    final byte[] qq = "tBSAF.qq".getBytes();

    // write a row for deletion within the batch, and one compareAndSwap
    Write write = new Write(keyB, kvcol, "0".getBytes());
    remote.commit(context, write);
    write = new Write(keyD, kvcol, "0".getBytes());
    remote.commit(context, write);
    QueueConfig config=new QueueConfig(PartitionerType.FIFO, true);
    QueueConsumer consumer = new QueueConsumer(0, 1, 1, config);
    remote.execute(context, null, new QueueAdmin.QueueConfigure(q, config, 1, 1));
    remote.execute(context, null, new QueueAdmin.QueueConfigure(qq, config, 1, 1));

    // insert two elements into a queue, and dequeue one to get an ack
    remote.commit(context, new QueueEnqueue(q, new QueueEntry("0".getBytes())));
    remote.commit(context, new QueueEnqueue(q, new QueueEntry("1".getBytes())));
    QueueDequeue dequeue = new QueueDequeue(q, consumer, config);
    DequeueResult dequeueResult = remote.execute(context, dequeue);
    Assert.assertNotNull(dequeueResult);
    Assert.assertTrue(dequeueResult.isSuccess());
    Assert.assertFalse(dequeueResult.isEmpty());
    Assert.assertArrayEquals("0".getBytes(), dequeueResult.getEntry().getData());

    // create a batch of write, delete, increment, enqueue, ack, compareAndSwap
    List<WriteOperation> writes = Lists.newArrayList();
    writes.add(new Write(keyA, kvcol, "1".getBytes()));
    writes.add(new Delete(keyB, kvcol));
    writes.add(new Increment(keyC, kvcol, 5));
    writes.add(new QueueEnqueue(qq, new QueueEntry("1".getBytes())));
    writes.add(new QueueAck(
        q, dequeueResult.getEntryPointer(), consumer));
    writes.add(new CompareAndSwap(
        keyD, Operation.KV_COL, "1".getBytes(), "2".getBytes()));

    // execute the writes and verify it failed (compareAndSwap must fail)
    try {
      remote.commit(context, writes);
      Assert.fail("expected coompare-and-swap conflict");
    } catch (OperationException e) {
      // expected
    }

    // verify that all operations were rolled back
    Assert.assertTrue(remote.execute(context, new Read(keyA, kvcol)).isEmpty());
    Assert.assertArrayEquals("0".getBytes(),
        remote.execute(context, new Read(keyB, kvcol)).getValue().get(kvcol));
    Assert.assertTrue(remote.execute(context, new Read(keyC, kvcol)).isEmpty());
    Assert.assertArrayEquals("0".getBytes(),
        remote.execute(context, new Read(keyD, kvcol)).getValue().get(kvcol));
    Assert.assertTrue(remote.execute(context,
        new QueueDequeue(qq, consumer, config)).isEmpty());
    // queue should return the same element until it is acked
    dequeueResult = remote.execute(context,
        new QueueDequeue(q, consumer, config));
    Assert.assertTrue(dequeueResult.isSuccess());
    Assert.assertFalse(dequeueResult.isEmpty());
    Assert.assertArrayEquals("0".getBytes(), dequeueResult.getEntry().getData());

    // set d to 1 to make compareAndSwap succeed
    remote.commit(context, new Write(keyD, kvcol, "1".getBytes()));

    // execute the writes again and verify it suceeded
    remote.commit(context, writes);

    // verify that all operations were performed
    Assert.assertArrayEquals("1".getBytes(),
        remote.execute(context, new Read(keyA, kvcol)).getValue().get(kvcol));
    Assert.assertTrue(remote.execute(context, new Read(keyB, kvcol)).isEmpty());
    Assert.assertArrayEquals(new byte[] { 0,0,0,0,0,0,0,5 },
        remote.execute(context, new Read(keyC, kvcol)).getValue().get(kvcol));
    Assert.assertArrayEquals("2".getBytes(),
        remote.execute(context, new Read(keyD, kvcol)).getValue().get(kvcol));
    dequeueResult = remote.execute(context,
        new QueueDequeue(qq, consumer, config));
    Assert.assertTrue(dequeueResult.isSuccess());
    Assert.assertFalse(dequeueResult.isEmpty());
    Assert.assertArrayEquals("1".getBytes(), dequeueResult.getEntry().getData());
    // queue should return the next element now that the previous one is acked
    dequeueResult = remote.execute(context,
        new QueueDequeue(q, consumer, config));
    Assert.assertTrue(dequeueResult.isSuccess());
    Assert.assertFalse(dequeueResult.isEmpty());
    Assert.assertArrayEquals("1".getBytes(), dequeueResult.getEntry().getData());
  }

  /** test clearFabric */
  @Test
  public void testClearFabric() throws Exception {
    final byte[] a = "tCFa".getBytes();
    final byte[] x = { 'x' };
    final byte[] q = "queue://tCF/q".getBytes();
    final byte[] s = "stream://tCF/s".getBytes();

    // write to a table, a queue, and a stream
    remote.commit(context, new Write(a, kvcol, x));
    remote.commit(context, new QueueEnqueue(q, new QueueEntry(x)));
    remote.commit(context, new QueueEnqueue(s, new QueueEntry(x)));

    // clear everything
    remote.execute(context, new ClearFabric(ClearFabric.ToClear.ALL));

    // verify that all is gone
    Assert.assertTrue(remote.execute(context, new Read(a, kvcol)).isEmpty());
    QueueConfig config = new QueueConfig(PartitionerType.FIFO, true);
    QueueConsumer consumer = new QueueConsumer(0, 1, 1, config);
    remote.execute(context, null, new QueueAdmin.QueueConfigure(q, config, 1, 1));
    Assert.assertTrue(remote.execute(
        context, new QueueDequeue(q, consumer, config)).isEmpty());
    Assert.assertTrue(remote.execute(
        context, new QueueDequeue(s, consumer, config)).isEmpty());

    // write back all values
    remote.commit(context, new Write(a, kvcol, x));
    remote.commit(context, new QueueEnqueue(q, new QueueEntry(x)));
    remote.commit(context, new QueueEnqueue(s, new QueueEntry(x)));

    // clear only the data
    remote.execute(context, new ClearFabric(ClearFabric.ToClear.DATA));

    // verify that the tables are gone, but queues and streams are there
    Assert.assertTrue(remote.execute(context, new Read(a, kvcol)).isEmpty());
    Assert.assertArrayEquals(x, remote.execute(
        context, new QueueDequeue(q, consumer, config)).getEntry().getData());
    Assert.assertArrayEquals(x, remote.execute(
        context, new QueueDequeue(s, consumer, config)).getEntry().getData());

    // write back to the table
    remote.commit(context, new Write(a, kvcol, x));

    // clear only the queues
    remote.execute(context, new ClearFabric(ClearFabric.ToClear.QUEUES));
    remote.execute(context, null, new QueueAdmin.QueueConfigure(q, config, 1, 1));

    // verify that the queues are gone, but tables and streams are there
    Assert.assertArrayEquals(x,
        remote.execute(context, new Read(a, kvcol)).getValue().get(kvcol));
    Assert.assertTrue(remote.execute(
        context, new QueueDequeue(q, consumer, config)).isEmpty());
    Assert.assertArrayEquals(x, remote.execute(
        context, new QueueDequeue(s, consumer, config)).getEntry().getData());

    // write back to the queue
    remote.commit(context, new QueueEnqueue(q, new QueueEntry(x)));

    // clear only the streams
    remote.execute(context, new ClearFabric(ClearFabric.ToClear.STREAMS));

    // verify that the streams are gone, but tables and queues are there
    Assert.assertArrayEquals(x, remote.execute(context, new Read(a, kvcol)).getValue().get(kvcol));
    Assert.assertArrayEquals(x, remote.execute(
        context, new QueueDequeue(q, consumer, config)).getEntry().getData());
    Assert.assertTrue(remote.execute(
        context, new QueueDequeue(s, consumer, config)).isEmpty());
  }

  /** tests enqueue, getGroupId and dequeue with ack for different groups */
  @Test
  public void testEnqueueThenDequeueAndAckWithDifferentGroups() throws Exception {
    final byte[] q = "queue://tWTDAAWDG/q".getBytes();
    final String HASH_KEY = "HashKey";

    // enqueue a bunch of entries, each one twice.
    // why twice? with hash partitioner, the same value will go to the same
    // consumer twice. With random partitioner, they go in the order of request
    // insert enough to be sure that even with hash partitioning, none of the
    // consumers will run out of entries to dequeue
    Random rand = new Random(42);
    int prev = 0, i = 0;
    while (i < 100) {
      int next = rand.nextInt(1000);
      if (next == prev) continue;
      byte[] value = Integer.toString(next).getBytes();
      QueueEntry entry =new QueueEntry(value);
      entry.addPartitioningKey(HASH_KEY, next);
      QueueEnqueue enqueue = new QueueEnqueue(q, entry);
      remote.commit(context, enqueue);
      remote.commit(context, enqueue);
      prev = next;
      i++;
    }
    // get two groupids
    long id1 = remote.execute(context, new QueueAdmin.GetGroupID(q));
    long id2 = remote.execute(context, new QueueAdmin.GetGroupID(q));
    Assert.assertFalse(id1 == id2);

    // creeate two configs, one hash, one random, one single, one multi
    QueueConfig conf1 = new QueueConfig(PartitionerType.HASH, false, 1);
    QueueConfig conf2 = new QueueConfig(PartitionerType.FIFO, true, 1);

    // create 2 consumers for each groupId
    QueueConsumer cons11 = new StatefulQueueConsumer(0, id1, 2, "", HASH_KEY, conf1, false);
    QueueConsumer cons12 = new StatefulQueueConsumer(1, id1, 2, "", HASH_KEY, conf1, false);
    QueueConsumer cons21 = new QueueConsumer(0, id2, 2, conf2);
    QueueConsumer cons22 = new QueueConsumer(1, id2, 2, conf2);

    // configure queues
    remote.execute(context, null, new QueueAdmin.QueueConfigure(q, conf1, id1, 2));
    remote.execute(context, null, new QueueAdmin.QueueConfigure(q, conf2, id2, 2));

    // dequeue with each consumer
    DequeueResult res11 = remote.execute(context, new QueueDequeue(q, cons11, conf1));
    DequeueResult res12 = remote.execute(context, new QueueDequeue(q, cons12, conf1));
    DequeueResult res21 = remote.execute(context, new QueueDequeue(q, cons21, conf2));
    DequeueResult res22 = remote.execute(context, new QueueDequeue(q, cons22, conf2));

    // verify that all results are successful
    Assert.assertTrue(res11.isSuccess() && !res11.isEmpty());
    Assert.assertTrue(res12.isSuccess() && !res12.isEmpty());
    Assert.assertTrue(res21.isSuccess() && !res21.isEmpty());
    Assert.assertTrue(res22.isSuccess() && !res22.isEmpty());

    // verify that the values from group 1 are different (hash partitioner)
    Assert.assertFalse(Arrays.equals(res11.getEntry().getData(), res12.getEntry().getData()));
    // and that the two values for group 2 are equal (random partitioner)
    Assert.assertArrayEquals(res21.getEntry().getData(), res22.getEntry().getData());

    // verify that group1 (multi-entry config) can dequeue more elements
    DequeueResult next11 =  remote.execute(context, new QueueDequeue(q, cons11, conf1));
    Assert.assertTrue(next11.isSuccess() && !next11.isEmpty());
    // for the second read we expect the same value again (enqueued twice)
    Assert.assertArrayEquals(res11.getEntry().getData(), next11.getEntry().getData());
    // but if we dequeue again, we should see a different one.
    next11 = remote.execute(context, new QueueDequeue(q, cons11, conf1));
    Assert.assertTrue(next11.isSuccess() && !next11.isEmpty());
    Assert.assertFalse(Arrays.equals(res11.getEntry().getData(), next11.getEntry().getData()));

    // verify that group2 (single-entry config) cannot dequeue more elements
    DequeueResult next21 = remote.execute(context, new QueueDequeue(q, cons21, conf2));
    Assert.assertTrue(next21.isSuccess() && !next21.isEmpty());
    // other than for group1 above, we would see a different value right
    // away (because the first two, identical value have been dequeued)
    // but this queue is in single-entry mode and requires an ack before
    // the next element can be read. Thus we should see the same value
    Assert.assertArrayEquals(res21.getEntry().getData(), next21.getEntry().getData());
    // just to be sure, do it again
    next21 = remote.execute(context, new QueueDequeue(q, cons21, conf2));
    Assert.assertTrue(next21.isSuccess() && !next21.isEmpty());
    Assert.assertArrayEquals(res21.getEntry().getData(), next21.getEntry().getData());

    // ack group 1 to verify that it did not affect group 2
    QueueEntryPointer pointer11 = res11.getEntryPointer();
    remote.commit(context, new QueueAck(q, pointer11, cons11));
    // dequeue group 2 again
    next21 = remote.execute(context, new QueueDequeue(q, cons21, conf2));
    Assert.assertTrue(next21.isSuccess() && !next21.isEmpty());
    Assert.assertArrayEquals(res21.getEntry().getData(), next21.getEntry().getData());
    // just to be sure, do it twice
    next21 = remote.execute(context, new QueueDequeue(q, cons21, conf2));
    Assert.assertTrue(next21.isSuccess() && !next21.isEmpty());
    Assert.assertArrayEquals(res21.getEntry().getData(), next21.getEntry().getData());

    // ack group 2, consumer 1,
    QueueEntryPointer pointer21 = res21.getEntryPointer();
    remote.commit(context, new QueueAck(q, pointer21, cons21));
    // dequeue group 2 again
    next21 = remote.execute(context, new QueueDequeue(q, cons21, conf2));
    Assert.assertTrue(next21.isSuccess() && !next21.isEmpty());
    Assert.assertFalse(Arrays.equals(res21.getEntry().getData(), next21.getEntry().getData()));

    // verify that consumer 2 of group 2 can still not see new entries
    DequeueResult next22 = remote.execute(context, new QueueDequeue(q, cons22, conf2));
    Assert.assertTrue(next22.isSuccess() && !next22.isEmpty());
    Assert.assertArrayEquals(res22.getEntry().getData(), next22.getEntry().getData());

    // get queue info with remote and opex, verify they are equal
    GetQueueInfo getQueueInfo = new GetQueueInfo(q);
    QueueInfo infoLocal = local.execute(context, getQueueInfo).getValue();
    QueueInfo infoRemote = remote.execute(context, getQueueInfo).getValue();
    //System.err.println(infoLocal);
    Assert.assertNotNull(infoLocal);
    Assert.assertNotNull(infoRemote);
    Assert.assertEquals(infoLocal, infoRemote);
  }

  /*
   * Test that the remote opex is thread safe:
   * Run many threads that perform reads and writes concurrently.
   * If the opex is not thread-safe, some of them will corrupt each other's
   * network communication.
   */
  @Test
  public void testMultiThreaded() {
    int numThreads = 5;
    int numWritesPerThread = 50;

    OpexThread[] threads = new OpexThread[numThreads];

    for (int i = 0; i < numThreads; i++) {
      OpexThread ti = new OpexThread(i, numWritesPerThread);
      Log.debug("Starting thread " + i);
      ti.start();
      Log.debug("Thread " + i + " is running");
      threads[i] = ti;
    }
    for (int i = 0; i < numThreads; i++) {
      try {
        threads[i].join();
        Assert.assertEquals(numWritesPerThread, threads[i].count);
      } catch (InterruptedException e) {
        e.printStackTrace();
        Assert.fail("join with thread " + i + " was interrupted");
      }
    }
  }

  class OpexThread extends Thread {
    int times;
    int id;
    int count = 0;
    OpexThread(int id, int times) {
      this.id = id;
      this.times = times;
    }
    public void run() {
      try {
        for (int i = 0; i < this.times; i++) {
          byte[] key = (id + "-" + i).getBytes();
          byte[] value = Integer.toString(i).getBytes();
          Log.debug("Thread " + id + " writing #" + i);
          Write write = new Write(key, Operation.KV_COL, value);
          remote.commit(context, write);
          Log.debug("Thread " + id + " reading #" + i);
          Read read = new Read(key, Operation.KV_COL);
          Assert.assertArrayEquals(value,
              remote.execute(context, read).getValue().get(Operation.KV_COL));
          count++;
        }
      } catch (Exception e) {
        Assert.fail("Exception in thread " + id + ": " + e.getMessage());
      }
    }
  }

}


