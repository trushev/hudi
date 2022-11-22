/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.table.format;

import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.internal.schema.convert.AvroInternalSchemaConverter;
import org.apache.hudi.internal.schema.utils.InternalSchemaUtils;
import org.apache.hudi.util.AvroSchemaConverter;

import org.apache.avro.Schema;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.table.types.logical.LogicalTypeRoot.BIGINT;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.DATE;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.DECIMAL;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.DOUBLE;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.FLOAT;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.INTEGER;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.VARCHAR;

/**
 * CastMap is responsible for conversion of flink types when full schema evolution enabled.
 * Supported cast conversions:
 * Integer => Long, Float, Double, Decimal, String
 * Long => Float, Double, Decimal, String
 * Float => Double, Decimal, String
 * Double => Decimal, String
 * Decimal => Decimal, String
 * String => Decimal, Date
 * Date => String
 */
public final class CastMap implements Serializable {
  private static final long serialVersionUID = 1L;

  // Maps position to corresponding cast
  private final Map<Integer, Cast> castMap = new HashMap<>();

  /**
   * Creates CastMap by comparing two schemes. Cast of a specific column is created if its type has changed.
   */
  public static CastMap of(String tableName, InternalSchema querySchema, InternalSchema actualSchema) {
    DataType queryType = internalSchemaToDataType(tableName, querySchema);
    DataType actualType = internalSchemaToDataType(tableName, actualSchema);
    CastMap castMap = new CastMap();
    InternalSchemaUtils.collectTypeChangedCols(querySchema, actualSchema).entrySet().stream()
        .filter(e -> !InternalSchemaUtils.isSameType(e.getValue().getLeft(), e.getValue().getRight()))
        .forEach(e -> {
          int pos = e.getKey();
          LogicalType target = queryType.getChildren().get(pos).getLogicalType();
          LogicalType actual = actualType.getChildren().get(pos).getLogicalType();
          castMap.add(pos, actual, target);
        });
    return castMap;
  }

  public Object castIfNeeded(int pos, Object val) {
    Cast cast = castMap.get(pos);
    if (cast == null) {
      return val;
    }
    return cast.convert(val);
  }

  public boolean containsAnyPos(int[] positions) {
    return Arrays.stream(positions).anyMatch(castMap.keySet()::contains);
  }

  public CastMap withNewPositions(int[] oldPositions, int[] newPositions) {
    Preconditions.checkArgument(oldPositions.length == newPositions.length);
    for (int i = 0; i < oldPositions.length; i++) {
      Cast cast = castMap.remove(oldPositions[i]);
      if (cast != null) {
        castMap.put(newPositions[i], cast);
      }
    }
    return this;
  }

  @VisibleForTesting
  void add(int pos, LogicalType fromType, LogicalType toType) {
    Function<Object, Object> conversion = getConversion(fromType, toType);
    if (conversion == null) {
      throw new IllegalArgumentException(String.format("Cannot create cast %s => %s at pos %s", fromType, toType, pos));
    }
    add(pos, new Cast(fromType, toType, conversion));
  }

  private Function<Object, Object> getConversion(LogicalType fromType, LogicalType toType) {
    LogicalTypeRoot from = fromType.getTypeRoot();
    LogicalTypeRoot to = toType.getTypeRoot();
    switch (to) {
      case BIGINT: {
        if (from == INTEGER) {
          return val -> ((Number) val).longValue();
        }
        break;
      }
      case FLOAT: {
        if (from == INTEGER || from == BIGINT) {
          return val -> ((Number) val).floatValue();
        }
        break;
      }
      case DOUBLE: {
        if (from == INTEGER || from == BIGINT) {
          return val -> ((Number) val).doubleValue();
        }
        if (from == FLOAT) {
          return val -> Double.parseDouble(val.toString());
        }
        break;
      }
      case DECIMAL: {
        if (from == INTEGER || from == BIGINT || from == DOUBLE) {
          return val -> toDecimalData((Number) val, toType);
        }
        if (from == FLOAT) {
          return val -> toDecimalData(Double.parseDouble(val.toString()), toType);
        }
        if (from == VARCHAR) {
          return val -> toDecimalData(Double.parseDouble(val.toString()), toType);
        }
        if (from == DECIMAL) {
          return val -> toDecimalData(((DecimalData) val).toBigDecimal(), toType);
        }
        break;
      }
      case VARCHAR: {
        if (from == INTEGER
            || from == BIGINT
            || from == FLOAT
            || from == DOUBLE
            || from == DECIMAL) {
          return val -> new BinaryStringData(String.valueOf(val));
        }
        if (from == DATE) {
          return val -> new BinaryStringData(LocalDate.ofEpochDay(((Integer) val).longValue()).toString());
        }
        break;
      }
      case DATE: {
        if (from == VARCHAR) {
          return val -> (int) LocalDate.parse(val.toString()).toEpochDay();
        }
        break;
      }
      default:
    }
    return null;
  }

  private void add(int pos, Cast cast) {
    castMap.put(pos, cast);
  }

  private DecimalData toDecimalData(Number val, LogicalType decimalType) {
    BigDecimal valAsDecimal = BigDecimal.valueOf(val.doubleValue());
    return toDecimalData(valAsDecimal, decimalType);
  }

  private DecimalData toDecimalData(BigDecimal valAsDecimal, LogicalType decimalType) {
    return DecimalData.fromBigDecimal(
        valAsDecimal,
        ((DecimalType) decimalType).getPrecision(),
        ((DecimalType) decimalType).getScale());
  }

  private static DataType internalSchemaToDataType(String tableName, InternalSchema internalSchema) {
    Schema schema = AvroInternalSchemaConverter.convert(internalSchema, tableName);
    return AvroSchemaConverter.convertToDataType(schema);
  }

  /**
   * Fields {@link Cast#from} and {@link Cast#to} are redundant due to {@link Cast#convert(Object)} determines conversion.
   * However, it is convenient to debug {@link CastMap} when {@link Cast#toString()} prints types.
   */
  private static final class Cast implements Serializable {
    private static final long serialVersionUID = 1L;

    private final LogicalType from;
    private final LogicalType to;
    private final Function<Object, Object> conversion;

    Cast(LogicalType from, LogicalType to, Function<Object, Object> conversion) {
      this.from = from;
      this.to = to;
      this.conversion = conversion;
    }

    Object convert(Object val) {
      return conversion.apply(val);
    }

    @Override
    public String toString() {
      return from + " => " + to;
    }
  }

  @Override
  public String toString() {
    return castMap.entrySet().stream()
        .map(e -> e.getKey() + ": " + e.getValue())
        .collect(Collectors.joining(", ", "{", "}"));
  }
}
