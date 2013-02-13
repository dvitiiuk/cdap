package com.continuuity.io;

import com.continuuity.internal.io.ReflectionSchemaGenerator;
import com.continuuity.api.io.Schema;
import com.continuuity.api.io.SchemaTypeAdapter;
import com.continuuity.api.io.UnsupportedTypeException;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class SchemaTest {

  public final class Node {
    private int data;
    private List<Node> children;
  }

  public class Parent<T> {
    private T data;
    private ByteBuffer buffer;
  }

  public class Child<T> extends Parent<Map<String, T>> {
    private int height;
    private Node rootNode;
    private State state;
  }

  public enum State {
    OK, ERROR
  }


  @Test
  public void testGenerateSchema() throws UnsupportedTypeException {
    Schema schema = (new ReflectionSchemaGenerator()).generate((new TypeToken<Child<Node>>() {}).getType());

    Gson gson = new GsonBuilder()
      .registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
      .create();

    Assert.assertEquals(schema, gson.fromJson(gson.toJson(schema), Schema.class));
  }

  public final class Node2 {
    private int data;
    private List<Node2> children;
  }

  @Test
  public void testSchemaHash() throws UnsupportedTypeException {
    Schema s1 = new ReflectionSchemaGenerator().generate(Node.class);
    Schema s2 = new ReflectionSchemaGenerator().generate(Node2.class);

    Assert.assertArrayEquals(s1.getSchemaHash(), s2.getSchemaHash());
    Assert.assertEquals(s1, s2);

    Schema schema = (new ReflectionSchemaGenerator()).generate((new TypeToken<Child<Node>>() {}).getType());
    Assert.assertFalse(Arrays.equals(s1.getSchemaHash(), schema.getSchemaHash()));
  }

  public final class Node3 {
    private long data;
    private String tag;
    private List<Node3> children;
  }

  public final class Node4 {
    private String data;
  }

  @Test
  public void testCompatible() throws UnsupportedTypeException {
    Schema s1 = new ReflectionSchemaGenerator().generate(Node.class);
    Schema s2 = new ReflectionSchemaGenerator().generate(Node3.class);
    Schema s3 = new ReflectionSchemaGenerator().generate(Node4.class);

    Assert.assertNotEquals(s1, s2);
    Assert.assertTrue(s1.isCompatible(s2));
    Assert.assertFalse(s2.isCompatible(s1));

    Assert.assertTrue(s2.isCompatible(s3));
  }
}
