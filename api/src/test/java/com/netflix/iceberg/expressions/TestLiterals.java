package com.netflix.iceberg.expressions;

import java.nio.ByteBuffer;

public class TestLiterals {
  // This allows other tests to create literals, but only in test
  public static <T> ValueLiteral<T> from(T value) {
    return Literals.from(value);
  }

  public static <T extends CharSequence> ValueLiteral<CharSequence> from(T value) {
    return Literals.from(value);
  }

  public static ValueLiteral<ByteBuffer> from(byte[] value) {
    return Literals.from(value);
  }
}
