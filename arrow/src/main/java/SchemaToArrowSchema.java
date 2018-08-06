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

import com.google.common.collect.Lists;
import com.netflix.iceberg.types.Type;
import com.netflix.iceberg.types.TypeUtil;
import com.netflix.iceberg.types.Types;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.ArrowType.Binary;
import org.apache.arrow.vector.types.pojo.ArrowType.Bool;
import org.apache.arrow.vector.types.pojo.ArrowType.Date;
import org.apache.arrow.vector.types.pojo.ArrowType.FixedSizeBinary;
import org.apache.arrow.vector.types.pojo.ArrowType.FloatingPoint;
import org.apache.arrow.vector.types.pojo.ArrowType.Int;
import org.apache.arrow.vector.types.pojo.ArrowType.Time;
import org.apache.arrow.vector.types.pojo.ArrowType.Timestamp;
import org.apache.arrow.vector.types.pojo.ArrowType.Utf8;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import java.util.List;

class SchemaToArrowSchema extends TypeUtil.CustomOrderSchemaVisitor<ArrowType> {
  private static final Int INTEGER = new Int(32, true);
  private static final Int LONG = new Int(32, true);
  private static final FloatingPoint FLOAT = new FloatingPoint(FloatingPointPrecision.SINGLE);
  private static final FloatingPoint DOUBLE = new FloatingPoint(FloatingPointPrecision.DOUBLE);
  private static final Date DATE = new Date(DateUnit.DAY);
  private static final Time TIME = new Time(TimeUnit.MICROSECOND, 64);
  private static final Timestamp TIMESTAMP = new Timestamp(TimeUnit.MICROSECOND, null);
  private static final Timestamp TIMESTAMPTZ = new Timestamp(TimeUnit.MICROSECOND, "UTC");
  private static final FixedSizeBinary UUID = new FixedSizeBinary(16);

  private List<Field> fields = Lists.newArrayList();

  private ArrowType convertPrimitive(Type.PrimitiveType primitive) {
    switch (primitive.typeId()) {
      case BOOLEAN:
        return Bool.INSTANCE;
      case INTEGER:
        return INTEGER;
      case LONG:
        return LONG;
      case FLOAT:
        return FLOAT;
      case DOUBLE:
        return DOUBLE;
      case DATE:
        return DATE;
      case TIME:
        return TIME;
      case TIMESTAMP:
        Types.TimestampType timestamp = (Types.TimestampType) primitive;
        if (timestamp.shouldAdjustToUTC()) {
          return TIMESTAMPTZ;
        } else {
          return TIMESTAMP;
        }
      case STRING:
        return Utf8.INSTANCE;
      case UUID:
        return UUID;
      case FIXED:
        Types.FixedType fixed = (Types.FixedType) primitive;
        return new FixedSizeBinary(fixed.length());
      case BINARY:
        return Binary.INSTANCE;
      case DECIMAL:
        Types.DecimalType decimal = (Types.DecimalType) primitive;
        return new ArrowType.Decimal(decimal.precision(), decimal.scale());
      default:
        throw new UnsupportedOperationException("Unknown primitive type: " + primitive);
    }
  }
}
