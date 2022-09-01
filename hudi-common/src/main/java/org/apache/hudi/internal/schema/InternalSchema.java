/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.internal.schema;

import org.apache.avro.Schema;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.internal.schema.Types.Field;
import org.apache.hudi.internal.schema.Types.RecordType;
import org.apache.hudi.internal.schema.convert.AvroInternalSchemaConverter;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Internal schema for hudi table.
 * used to support schema evolution.
 */
public class InternalSchema implements Serializable {

  public static final long DEFAULT_VERSION_ID = 0;

  private static final InternalSchema EMPTY_SCHEMA = new InternalSchema(-1, Collections.emptyList());

  private final RecordType record;
  private transient Schema avroSchema;
  private final int maxColumnId;
  private long versionId;

  private final boolean evolutionEnabled;

  public static InternalSchema getEmptyInternalSchema() {
    return EMPTY_SCHEMA;
  }

  public InternalSchema(List<Field> columns) {
    this(DEFAULT_VERSION_ID, columns);
  }

  public InternalSchema(long versionId, List<Field> cols) {
    this(RecordType.get(cols), -1, versionId);
  }

  public InternalSchema(long versionId, int maxColumnId, List<Field> cols) {
    this(RecordType.get(cols), maxColumnId, versionId);
  }

  public InternalSchema(Schema avroSchema) {
    this(RecordType.get(Collections.emptyList()), avroSchema, -1, -1, false);
  }

  private InternalSchema(RecordType record, int maxColumnId, long versionId) {
    this(record,
        AvroInternalSchemaConverter.convert(record, "record"),
        maxColumnId != -1 ? maxColumnId : record.ids().stream().mapToInt(i -> i).max().orElse(-1),
        versionId,
        true);
  }

  private InternalSchema(RecordType record, Schema schema, int maxColumnId, long versionId, boolean evolutionEnabled) {
    this.record = record;
    this.avroSchema = schema;
    this.maxColumnId = maxColumnId;
    this.versionId = versionId;
    this.evolutionEnabled = evolutionEnabled;
  }

  public boolean isEmptySchema() {
    return versionId < 0 || record.fields().isEmpty();
  }

  public boolean isEvolutionEnabled() {
    return evolutionEnabled;
  }

  public RecordType getRecord() {
    return record;
  }

  public Schema getAvroSchema() {
    if (avroSchema == null) {
      avroSchema = AvroInternalSchemaConverter.convert(record, "record");
    }
    return avroSchema;
  }

  /**
   * Get all columns full name.
   */
  public List<String> getAllColsFullName() {
    return record.fieldNames();
  }

  /**
   * Set the version ID for this schema.
   */
  public InternalSchema setSchemaId(long versionId) {
    this.versionId = versionId;
    return this;
  }

  /**
   * Returns the version ID for this schema.
   */
  public long schemaId() {
    return this.versionId;
  }

  /**
   * Returns the max column id for this schema.
   */
  public int getMaxColumnId() {
    return this.maxColumnId;
  }

  /**
   * Returns a List of the {@link Field columns} in this Schema.
   */
  public List<Field> columns() {
    return record.fields();
  }

  /**
   * Returns the {@link Type} of a sub-field identified by the field name.
   *
   * @param id a field id
   * @return fullName of field of
   */
  public String findfullName(int id) {
    return record.fieldName(id);
  }

  /**
   * Returns the {@link Type} of a sub-field identified by the field name.
   *
   * @param name a field name
   * @return a Type for the sub-field or null if it is not found
   */
  public Type findType(String name) {
    return record.fieldType(name);
  }

  /**
   * Returns the {@link Type} of a sub-field identified by the field id.
   *
   * @param id a field id
   * @return a Type for the sub-field or null if it is not found
   */
  public Type findType(int id) {
    Field field = record.field(id);
    if (field == null) {
      return null;
    }
    return field.type();
  }

  /**
   * Returns all field ids
   */
  public Set<Integer> getAllIds() {
    return record.ids();
  }

  /**
   * Returns the sub-field identified by the field id.
   *
   * @param id a field id
   * @return the sub-field or null if it is not found
   */
  public Field findField(int id) {
    return record.field(id);
  }

  /**
   * Returns a sub-field by name as a {@link Field}.
   * The result may be a top-level or a nested field.
   *
   * @param name a String name
   * @return a Type for the sub-field or null if it is not found
   */
  public Field findField(String name) {
    return record.field(name);
  }

  /**
   * Whether colName exists in current Schema.
   * Case insensitive.
   *
   * @param colName a colName
   * @return Whether colName exists in current Schema
   */
  public boolean findDuplicateCol(String colName) {
    return record.field(colName) != null;
  }

  public int findIdByName(String name) {
    return record.id(name);
  }

  @Override
  public String toString() {
    return String.format("table {\n%s\n}",
        StringUtils.join(record.fields().stream()
            .map(f -> " " + f)
            .collect(Collectors.toList()).toArray(new String[0]), "\n"));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    } else if (!(o instanceof InternalSchema)) {
      return false;
    }
    InternalSchema that = (InternalSchema) o;
    if (versionId != that.schemaId()) {
      return false;
    }
    return record.equals(that.record);
  }

  @Override
  public int hashCode() {
    return record.hashCode();
  }
}
