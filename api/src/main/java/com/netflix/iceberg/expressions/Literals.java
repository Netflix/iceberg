/*
 * Copyright 2017 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.iceberg.expressions;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.netflix.iceberg.types.Comparators;
import com.netflix.iceberg.types.Type;
import com.netflix.iceberg.types.Types;
import java.io.ObjectStreamException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

class Literals {
  private Literals() {
  }

  private static final OffsetDateTime EPOCH = Instant.ofEpochSecond(0).atOffset(ZoneOffset.UTC);
  private static final LocalDate EPOCH_DAY = EPOCH.toLocalDate();

  static <T extends CharSequence> ValueLiteral<CharSequence> from(T value) {
    return new StringLiteral(value);
  }

  static ValueLiteral<ByteBuffer> from(byte[] value) {
    return new FixedLiteral(ByteBuffer.wrap(value));
  }

  /**
   * Create a {@link Literal} from an Object.
   *
   * @param value a value
   * @param <T> Java type of value
   * @return a Literal for the given value
   */
  @SuppressWarnings("unchecked")
  static <T> ValueLiteral<T> from(T value) {
    Preconditions.checkNotNull(value, "Cannot create expression literal from null");

    if (value instanceof Boolean) {
      return (ValueLiteral<T>) new Literals.BooleanLiteral((Boolean) value);
    } else if (value instanceof Integer) {
      return (ValueLiteral<T>) new Literals.IntegerLiteral((Integer) value);
    } else if (value instanceof Long) {
      return (ValueLiteral<T>) new Literals.LongLiteral((Long) value);
    } else if (value instanceof Float) {
      return (ValueLiteral<T>) new Literals.FloatLiteral((Float) value);
    } else if (value instanceof Double) {
      return (ValueLiteral<T>) new Literals.DoubleLiteral((Double) value);
    } else if (value instanceof CharSequence) {
      return (ValueLiteral<T>) new Literals.StringLiteral((CharSequence) value);
    } else if (value instanceof UUID) {
      return (ValueLiteral<T>) new Literals.UUIDLiteral((UUID) value);
    } else if (value instanceof byte[]) {
      return (ValueLiteral<T>) new Literals.FixedLiteral(ByteBuffer.wrap((byte[]) value));
    } else if (value instanceof ByteBuffer) {
      return (ValueLiteral<T>) new Literals.BinaryLiteral((ByteBuffer) value);
    } else if (value instanceof BigDecimal) {
      return (ValueLiteral<T>) new Literals.DecimalLiteral((BigDecimal) value);
    }

    throw new IllegalArgumentException(String.format(
        "Cannot create expression value literal from %s: %s", value.getClass().getName(), value));
  }

  @SuppressWarnings("unchecked")
  static <T> AboveMax<T> aboveMax() {
    return AboveMax.INSTANCE;
  }

  @SuppressWarnings("unchecked")
  static <T> BelowMin<T> belowMin() {
    return BelowMin.INSTANCE;
  }

  private abstract static class BaseLiteral<T> implements ValueLiteral<T> {
    private final T value;

    BaseLiteral(T value) {
      this.value = value;
    }

    @Override
    public T value() {
      return value;
    }

    @Override
    public String show() {
      return String.valueOf(value);
    }
  }

  private abstract static class ComparableLiteral<C extends Comparable<C>> extends BaseLiteral<C> {
    @SuppressWarnings("unchecked")
    private static final Comparator<? extends Comparable> CMP =
        Comparators.<Comparable>nullsFirst().thenComparing(Comparator.naturalOrder());

    public ComparableLiteral(C value) {
      super(value);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Comparator<C> comparator() {
      return (Comparator<C>) CMP;
    }
  }

  static class AboveMax<T> implements ValueLiteral<T> {
    private static final AboveMax INSTANCE = new AboveMax();

    private AboveMax() {
    }

    @Override
    public T value() {
      throw new UnsupportedOperationException("AboveMax has no value");
    }

    @Override
    public <X> ValueLiteral<X> to(Type type) {
      throw new UnsupportedOperationException("Cannot change the type of AboveMax");
    }

    @Override
    public Comparator<T> comparator() {
      throw new UnsupportedOperationException("AboveMax has no comparator");
    }

    @Override
    public String show() {
      return "AboveMax";
    }

    @Override
    public String toString() {
      return "aboveMax";
    }
  }

  static class BelowMin<T> implements ValueLiteral<T> {
    private static final BelowMin INSTANCE = new BelowMin();

    private BelowMin() {
    }

    @Override
    public T value() {
      throw new UnsupportedOperationException("BelowMin has no value");
    }

    @Override
    public <X> ValueLiteral<X> to(Type type) {
      throw new UnsupportedOperationException("Cannot change the type of BelowMin");
    }

    @Override
    public Comparator<T> comparator() {
      throw new UnsupportedOperationException("BelowMin has no comparator");
    }

    @Override
    public String show() {
      return "BelowMin";
    }

    @Override
    public String toString() {
      return "belowMin";
    }
  }

  static class BooleanLiteral extends ComparableLiteral<Boolean> {
    BooleanLiteral(Boolean value) {
      super(value);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> ValueLiteral<T> to(Type type) {
      if (type.typeId() == Type.TypeID.BOOLEAN) {
        return (ValueLiteral<T>) this;
      }
      return null;
    }
  }

  static class IntegerLiteral extends ComparableLiteral<Integer> {
    IntegerLiteral(Integer value) {
      super(value);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> ValueLiteral<T> to(Type type) {
      switch (type.typeId()) {
        case INTEGER:
          return (ValueLiteral<T>) this;
        case LONG:
          return (ValueLiteral<T>) new LongLiteral(value().longValue());
        case FLOAT:
          return (ValueLiteral<T>) new FloatLiteral(value().floatValue());
        case DOUBLE:
          return (ValueLiteral<T>) new DoubleLiteral(value().doubleValue());
        case DATE:
          return (ValueLiteral<T>) new DateLiteral(value());
        case DECIMAL:
          int scale = ((Types.DecimalType) type).scale();
          // rounding mode isn't necessary, but pass one to avoid warnings
          return (ValueLiteral<T>) new DecimalLiteral(
              BigDecimal.valueOf(value()).setScale(scale, RoundingMode.HALF_UP));
        default:
          return null;
      }
    }
  }

  static class LongLiteral extends ComparableLiteral<Long> {
    LongLiteral(Long value) {
      super(value);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> ValueLiteral<T> to(Type type) {
      switch (type.typeId()) {
        case INTEGER:
          if ((long) Integer.MAX_VALUE < value()) {
            return aboveMax();
          } else if ((long) Integer.MIN_VALUE > value()) {
            return belowMin();
          }
          return (ValueLiteral<T>) new IntegerLiteral(value().intValue());
        case LONG:
          return (ValueLiteral<T>) this;
        case FLOAT:
          return (ValueLiteral<T>) new FloatLiteral(value().floatValue());
        case DOUBLE:
          return (ValueLiteral<T>) new DoubleLiteral(value().doubleValue());
        case TIME:
          return (ValueLiteral<T>) new TimeLiteral(value());
        case TIMESTAMP:
          return (ValueLiteral<T>) new TimestampLiteral(value());
        case DECIMAL:
          int scale = ((Types.DecimalType) type).scale();
          // rounding mode isn't necessary, but pass one to avoid warnings
          return (ValueLiteral<T>) new DecimalLiteral(
              BigDecimal.valueOf(value()).setScale(scale, RoundingMode.HALF_UP));
        default:
          return null;
      }
    }
  }

  static class FloatLiteral extends ComparableLiteral<Float> {
    FloatLiteral(Float value) {
      super(value);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> ValueLiteral<T> to(Type type) {
      switch (type.typeId()) {
        case FLOAT:
          return (ValueLiteral<T>) this;
        case DOUBLE:
          return (ValueLiteral<T>) new DoubleLiteral(value().doubleValue());
        case DECIMAL:
          int scale = ((Types.DecimalType) type).scale();
          return (ValueLiteral<T>) new DecimalLiteral(
              BigDecimal.valueOf(value()).setScale(scale, RoundingMode.HALF_UP));
        default:
          return null;
      }
    }
  }

  static class DoubleLiteral extends ComparableLiteral<Double> {
    DoubleLiteral(Double value) {
      super(value);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> ValueLiteral<T> to(Type type) {
      switch (type.typeId()) {
        case FLOAT:
          if ((double) Float.MAX_VALUE < value()) {
            return aboveMax();
          } else if ((double) -Float.MAX_VALUE > value()) {
            // Compare with -Float.MAX_VALUE because it is the most negative float value.
            // Float.MIN_VALUE is the smallest non-negative floating point value.
            return belowMin();
          }
          return (ValueLiteral<T>) new FloatLiteral(value().floatValue());
        case DOUBLE:
          return (ValueLiteral<T>) this;
        case DECIMAL:
          int scale = ((Types.DecimalType) type).scale();
          return (ValueLiteral<T>) new DecimalLiteral(
              BigDecimal.valueOf(value()).setScale(scale, RoundingMode.HALF_UP));
        default:
          return null;
      }
    }
  }

  static class DateLiteral extends ComparableLiteral<Integer> {
    DateLiteral(Integer value) {
      super(value);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> ValueLiteral<T> to(Type type) {
      if (type.typeId() == Type.TypeID.DATE) {
        return (ValueLiteral<T>) this;
      }
      return null;
    }
  }

  static class TimeLiteral extends ComparableLiteral<Long> {
    TimeLiteral(Long value) {
      super(value);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> ValueLiteral<T> to(Type type) {
      if (type.typeId() == Type.TypeID.TIME) {
        return (ValueLiteral<T>) this ;
      }
      return null;
    }
  }

  static class TimestampLiteral extends ComparableLiteral<Long> {
    TimestampLiteral(Long value) {
      super(value);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> ValueLiteral<T> to(Type type) {
      switch (type.typeId()) {
        case TIMESTAMP:
          return (ValueLiteral<T>) this;
        case DATE:
          return (ValueLiteral<T>) new DateLiteral((int) ChronoUnit.DAYS.between(
              EPOCH_DAY, EPOCH.plus(value(), ChronoUnit.MICROS).toLocalDate()));
        default:
      }
      return null;
    }
  }

  static class DecimalLiteral extends ComparableLiteral<BigDecimal> {
    DecimalLiteral(BigDecimal value) {
      super(value);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> ValueLiteral<T> to(Type type) {
      switch (type.typeId()) {
        case DECIMAL:
          // do not change decimal scale
          if (value().scale() == ((Types.DecimalType) type).scale()) {
            return (ValueLiteral<T>) this;
          }
          return null;
        default:
          return null;
      }
    }
  }

  static class StringLiteral extends BaseLiteral<CharSequence> {
    private static final Comparator<CharSequence> CMP =
        Comparators.<CharSequence>nullsFirst().thenComparing(Comparators.charSequences());

    StringLiteral(CharSequence value) {
      super(value);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> ValueLiteral<T> to(Type type) {
      switch (type.typeId()) {
        case DATE:
          int date = (int) ChronoUnit.DAYS.between(EPOCH_DAY,
              LocalDate.parse(value(), DateTimeFormatter.ISO_LOCAL_DATE));
          return (ValueLiteral<T>) new DateLiteral(date);

        case TIME:
          long timeMicros = LocalTime.parse(value(), DateTimeFormatter.ISO_LOCAL_TIME)
              .toNanoOfDay() / 1000;
          return (ValueLiteral<T>) new TimeLiteral(timeMicros);

        case TIMESTAMP:
          if (((Types.TimestampType) type).shouldAdjustToUTC()) {
            long timestampMicros = ChronoUnit.MICROS.between(EPOCH,
                OffsetDateTime.parse(value(), DateTimeFormatter.ISO_DATE_TIME));
            return (ValueLiteral<T>) new TimestampLiteral(timestampMicros);
          } else {
            long timestampMicros = ChronoUnit.MICROS.between(EPOCH,
                LocalDateTime.parse(value(), DateTimeFormatter.ISO_LOCAL_DATE_TIME)
                    .atOffset(ZoneOffset.UTC));
            return (ValueLiteral<T>) new TimestampLiteral(timestampMicros);
          }

        case STRING:
          return (ValueLiteral<T>) this;

        case UUID:
          return (ValueLiteral<T>) new UUIDLiteral(UUID.fromString(value().toString()));

        case DECIMAL:
          int scale = ((Types.DecimalType) type).scale();
          BigDecimal decimal = new BigDecimal(value().toString());
          if (scale == decimal.scale()) {
            return (ValueLiteral<T>) new DecimalLiteral(decimal);
          }
          return null;

        default:
          return null;
      }
    }

    @Override
    public Comparator<CharSequence> comparator() {
      return CMP;
    }

    @Override
    public String toString() {
      return "\"" + value() + "\"";
    }
  }

  static class UUIDLiteral extends ComparableLiteral<UUID> {
    UUIDLiteral(UUID value) {
      super(value);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> ValueLiteral<T> to(Type type) {
      if (type.typeId() == Type.TypeID.UUID) {
        return (ValueLiteral<T>) this;
      }
      return null;
    }
  }

  static class FixedLiteral extends BaseLiteral<ByteBuffer> {
    private static final Comparator<ByteBuffer> CMP =
        Comparators.<ByteBuffer>nullsFirst().thenComparing(Comparators.unsignedBytes());

    FixedLiteral(ByteBuffer value) {
      super(value);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> ValueLiteral<T> to(Type type) {
      switch (type.typeId()) {
        case FIXED:
          Types.FixedType fixed = (Types.FixedType) type;
          if (value().remaining() == fixed.length()) {
            return (ValueLiteral<T>) this;
          }
          return null;
        case BINARY:
          return (ValueLiteral<T>) new BinaryLiteral(value());
        default:
          return null;
      }
    }

    @Override
    public Comparator<ByteBuffer> comparator() {
      return CMP;
    }

    Object writeReplace() throws ObjectStreamException {
      return new SerializationProxies.FixedLiteralProxy(value());
    }
  }

  static class BinaryLiteral extends BaseLiteral<ByteBuffer> {
    private static final Comparator<ByteBuffer> CMP =
        Comparators.<ByteBuffer>nullsFirst().thenComparing(Comparators.unsignedBytes());

    BinaryLiteral(ByteBuffer value) {
      super(value);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> ValueLiteral<T> to(Type type) {
      switch (type.typeId()) {
        case FIXED:
          Types.FixedType fixed = (Types.FixedType) type;
          if (value().remaining() == fixed.length()) {
            return (ValueLiteral<T>) new FixedLiteral(value());
          }
          return null;
        case BINARY:
          return (ValueLiteral<T>) this;
        default:
          return null;
      }
    }

    @Override
    public Comparator<ByteBuffer> comparator() {
      return CMP;
    }

    Object writeReplace() throws ObjectStreamException {
      return new SerializationProxies.BinaryLiteralProxy(value());
    }
  }
  
  static class CollectionLiteralImpl<T> implements CollectionLiteral<T> {
    private final Collection<T> values;
  
    CollectionLiteralImpl(Collection<T> values) {
      this.values = values;
    }
  
    @Override
    public Collection<T> values() {
      return this.values;
    }
  
    @Override
    public Collection<ValueLiteral<T>> literalValues() {
      return this.values.stream().map(Literals::from).collect(Collectors.toList());
    }
  
    @Override
    public <X> CollectionLiteral<X> to(Type type) {
      List<X> converted = values.stream()
              .map(Literals::from)
              .map(l -> l.<X>to(type))
              .map(ValueLiteral::value)
              .collect(Collectors.toList());
      
      return new CollectionLiteralImpl<>(converted);
    }
  
    @Override
    public Comparator<T> comparator() {
      return values.stream()
              .findFirst()
              .map(Literals::from)
              .map(Literal::comparator)
              .orElse((o1, o2) -> {
                // This is not the right type to throw, but Java doesn't permit adding new types
                throw new ClassCastException("Comparator used with an instance other than the one " +
                        "it was created from, and the original instance was empty");
              });
    }

    @Override
    public String show() {
      return Joiner.on(", ").join(this.values());
    }
  }
}
