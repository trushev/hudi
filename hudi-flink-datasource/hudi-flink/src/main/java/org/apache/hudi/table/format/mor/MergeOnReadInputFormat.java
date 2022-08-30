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

package org.apache.hudi.table.format.mor;

import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieOperation;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.log.HoodieMergedLogRecordScanner;
import org.apache.hudi.common.table.log.InstantRange;
import org.apache.hudi.common.util.ClosableIterator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.configuration.HadoopConfigurations;
import org.apache.hudi.configuration.OptionsResolver;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.internal.schema.convert.AvroInternalSchemaConverter;
import org.apache.hudi.keygen.KeyGenUtils;
import org.apache.hudi.table.format.*;
import org.apache.hudi.table.format.cow.ParquetSplitReaderUtil;
import org.apache.hudi.table.format.cow.vector.reader.ParquetColumnarRowSplitReader;
import org.apache.hudi.util.AvroToRowDataConverters;
import org.apache.hudi.util.DataTypeUtils;
import org.apache.hudi.util.RowDataCastProjection;
import org.apache.hudi.util.RowDataProjection;
import org.apache.hudi.util.RowDataToAvroConverters;
import org.apache.hudi.util.StreamerUtil;
import org.apache.hudi.util.StringToRowDataConverter;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.generic.IndexedRecord;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.stream.IntStream;

import static org.apache.hudi.hadoop.utils.HoodieInputFormatUtils.HOODIE_COMMIT_TIME_COL_POS;
import static org.apache.hudi.hadoop.utils.HoodieInputFormatUtils.HOODIE_RECORD_KEY_COL_POS;
import static org.apache.hudi.table.format.FormatUtils.buildAvroRecordBySchema;

/**
 * The base InputFormat class to read from Hoodie data + log files.
 *
 * <P>Use {@code ParquetRecordReader} to read files instead of {@link org.apache.flink.core.fs.FSDataInputStream},
 * overrides {@link #createInputSplits(int)} and {@link #close()} to change the behaviors.
 */
public class MergeOnReadInputFormat
    extends RichInputFormat<RowData, MergeOnReadInputSplit> {

  private static final long serialVersionUID = 1L;

  private final Configuration conf;

  private transient org.apache.hadoop.conf.Configuration hadoopConf;

  private final MergeOnReadTableState tableState;

  /**
   * Uniform iterator view for the underneath records.
   */
  private transient RecordIterator iterator;

  // for project push down
  /**
   * Full table names.
   */
  private final List<String> fieldNames;

  /**
   * Full field data types.
   */
  private final List<DataType> fieldTypes;

  /**
   * Default partition name when the field value is null.
   */
  private final String defaultPartName;

  /**
   * Required field positions.
   */
  private final int[] requiredPos;

  // for limit push down
  /**
   * Limit for the reader, -1 when the reading is not limited.
   */
  private final long limit;

  /**
   * Recording the current read count for limit check.
   */
  private long currentReadCount = 0;

  /**
   * Flag saying whether to emit the deletes. In streaming read mode, downstream
   * operators need the DELETE messages to retract the legacy accumulator.
   */
  private boolean emitDelete;

  /**
   * Flag saying whether the input format has been closed.
   */
  private boolean closed = true;

  private final Option<SchemaEvolutionContext> schemaEvolutionContext;
  private List<String> actualFieldNames;
  private List<DataType> actualFieldTypes;
  private InternalSchema actualSchema;
  private InternalSchema querySchema;

  public MergeOnReadInputFormat(
      Configuration conf,
      MergeOnReadTableState tableState,
      List<DataType> fieldTypes,
      String defaultPartName,
      long limit,
      boolean emitDelete) {
    this.conf = conf;
    this.tableState = tableState;
    this.fieldNames = tableState.getRowType().getFieldNames();
    this.fieldTypes = fieldTypes;
    this.defaultPartName = defaultPartName;
    // Needs improvement: this requiredPos is only suitable for parquet reader,
    // because we need to
    this.requiredPos = tableState.getRequiredPositions();
    this.limit = limit;
    this.emitDelete = emitDelete;
    this.schemaEvolutionContext = SchemaEvolutionContext.of(conf);
  }

  @Override
  public void open(MergeOnReadInputSplit split) throws IOException {
    this.currentReadCount = 0L;
    this.closed = false;
    this.hadoopConf = HadoopConfigurations.getHadoopConf(this.conf);
    if (schemaEvolutionContext.isPresent()) {
      SchemaEvolutionContext context = schemaEvolutionContext.get();
      querySchema = context.getQuerySchema();
      actualSchema = context.getActualSchema(split);
      actualFieldNames = context.getFieldNames(actualSchema);
      actualFieldTypes = context.getFieldTypes(actualSchema);
    } else {
      querySchema = InternalSchema.getEmptyInternalSchema();
      actualSchema = InternalSchema.getEmptyInternalSchema();
      actualFieldNames = fieldNames;
      actualFieldTypes = fieldTypes;
    }

    IteratorType iteratorType = IteratorType.getIteratorType(split, conf);
    switch (iteratorType) {
      case BASE_FILE_ONLY_FILTERING: {
        // base file only with commit time filtering
        this.iterator = new BaseFileOnlyFilteringIterator(
            split.getInstantRange().get(),
            getReader(split.getBasePath().get(), getRequiredPosWithCommitTime(this.requiredPos)));
        int[] positions = IntStream.range(1, requiredPos.length + 1).toArray();
        RowDataProjection projection = getCastProjection(positions)
            .orElse(RowDataProjection.instance(tableState.getRequiredRowType(), positions));
        projectRecordIterator(projection);
        break;
      }
      case BASE_FILE_ONLY: {
        // base file only
        this.iterator = new BaseFileOnlyIterator(getRequiredSchemaReader(split.getBasePath().get()));
        projectRecordIterator();
        break;
      }
      case LOG_FILE_ONLY_UNMERGED: {
        this.iterator = new LogFileOnlyIterator(getUnMergedLogFileIterator(split));
        projectRecordIterator();
        break;
      }
      case LOG_FILE_ONLY: {
        this.iterator = new LogFileOnlyIterator(getLogFileIterator(split));
        projectRecordIterator();
        break;
      }
      case BASE_LOG_FILE_SKIP_MERGE: {
        RecordIterator baseFileIterator = new BaseFileOnlyIterator(getRequiredSchemaReader(split.getBasePath().get()));
        this.iterator = new SkipMergeIterator(
            getCastProjection().map(pr -> (RecordIterator) new RecordIterator.ProjectionIterator(baseFileIterator, pr)).orElse(baseFileIterator),
            getLogFileIterator(split));
        break;
      }
      case BASE_LOG_FILE_MERGE: {
        RowDataProjection projection = getCastProjection(requiredPos)
            .orElse(RowDataProjection.instance(tableState.getRequiredRowType(), requiredPos));
        Option<RowDataProjection> projectionBeforeMerge = schemaEvolutionContext.map(context -> {
          CastMap castMap = context.getCastMap(querySchema, actualSchema);
          int[] positions = IntStream.range(0, actualFieldTypes.size()).toArray();
          return new RowDataCastProjection(SchemaEvolutionContext.project(actualFieldTypes, positions), positions, castMap);
        });
        this.iterator = new MergeIterator(
            conf,
            hadoopConf,
            split,
            this.tableState.getRowType(),
            this.tableState.getRequiredRowType(),
            new Schema.Parser().parse(this.tableState.getAvroSchema()),
            new Schema.Parser().parse(this.tableState.getRequiredAvroSchema()),
            this.querySchema,
            projection,
            projectionBeforeMerge,
            this.requiredPos,
            this.emitDelete,
            this.tableState.getOperationPos(),
            getFullSchemaReader(split.getBasePath().get()));
        break;
      }
      default:
        throw new IllegalStateException(iteratorType.toString());
    }
    mayShiftInputSplit(split);
  }

  enum IteratorType {
    BASE_FILE_ONLY_FILTERING,
    BASE_FILE_ONLY,
    LOG_FILE_ONLY_UNMERGED,
    LOG_FILE_ONLY,
    BASE_LOG_FILE_SKIP_MERGE,
    BASE_LOG_FILE_MERGE;

    static IteratorType getIteratorType(MergeOnReadInputSplit split, Configuration conf) {
      if (!(split.getLogPaths().isPresent() && split.getLogPaths().get().size() > 0)) {
        if (split.getInstantRange().isPresent()) {
          return BASE_FILE_ONLY_FILTERING;
        } else {
          return BASE_FILE_ONLY;
        }
      } else if (!split.getBasePath().isPresent()) {
        if (OptionsResolver.emitChangelog(conf)) {
          return LOG_FILE_ONLY_UNMERGED;
        } else {
          return LOG_FILE_ONLY;
        }
      } else if (split.getMergeType().equals(FlinkOptions.REALTIME_SKIP_MERGE)) {
        return BASE_LOG_FILE_SKIP_MERGE;
      } else if (split.getMergeType().equals(FlinkOptions.REALTIME_PAYLOAD_COMBINE)) {
        return BASE_LOG_FILE_MERGE;
      } else {
        throw new HoodieException("Unable to select an Iterator to read the Hoodie MOR File Split for "
            + "file path: " + split.getBasePath()
            + "log paths: " + split.getLogPaths()
            + "hoodie table path: " + split.getTablePath()
            + "spark partition Index: " + split.getSplitNumber()
            + "merge type: " + split.getMergeType());
      }
    }
  }

  @Override
  public void configure(Configuration configuration) {
    // no operation
    // may support nested files in the future.
  }

  @Override
  public BaseStatistics getStatistics(BaseStatistics baseStatistics) {
    // statistics not supported yet.
    return null;
  }

  @Override
  public MergeOnReadInputSplit[] createInputSplits(int minNumSplits) {
    return this.tableState.getInputSplits().toArray(new MergeOnReadInputSplit[0]);
  }

  @Override
  public InputSplitAssigner getInputSplitAssigner(MergeOnReadInputSplit[] mergeOnReadInputSplits) {
    return new DefaultInputSplitAssigner(mergeOnReadInputSplits);
  }

  @Override
  public boolean reachedEnd() throws IOException {
    if (limit > 0 && currentReadCount >= limit) {
      return true;
    } else {
      // log file reaches end ?
      return this.iterator.reachedEnd();
    }
  }

  @Override
  public RowData nextRecord(RowData o) {
    currentReadCount++;
    return this.iterator.nextRecord();
  }

  @Override
  public void close() throws IOException {
    if (this.iterator != null) {
      this.iterator.close();
    }
    this.iterator = null;
    this.closed = true;
  }

  public boolean isClosed() {
    return this.closed;
  }

  // -------------------------------------------------------------------------
  //  Utilities
  // -------------------------------------------------------------------------

  /**
   * Shifts the input split by its consumed records number.
   *
   * <p>Note: This action is time-consuming.
   */
  private void mayShiftInputSplit(MergeOnReadInputSplit split) throws IOException {
    if (split.isConsumed()) {
      // if the input split has been consumed before,
      // shift the input split with consumed num of records first
      for (long i = 0; i < split.getConsumed() && !reachedEnd(); i++) {
        nextRecord(null);
      }
    }
  }

  private ParquetColumnarRowSplitReader getFullSchemaReader(String path) throws IOException {
    return getReader(path, IntStream.range(0, this.tableState.getRowType().getFieldCount()).toArray());
  }

  private ParquetColumnarRowSplitReader getRequiredSchemaReader(String path) throws IOException {
    return getReader(path, this.requiredPos);
  }

  private ParquetColumnarRowSplitReader getReader(String path, int[] requiredPos) throws IOException {
    // generate partition specs.
    LinkedHashMap<String, String> partSpec = FilePathUtils.extractPartitionKeyValues(
        new org.apache.hadoop.fs.Path(path).getParent(),
        this.conf.getBoolean(FlinkOptions.HIVE_STYLE_PARTITIONING),
        FilePathUtils.extractPartitionKeys(this.conf));
    LinkedHashMap<String, Object> partObjects = new LinkedHashMap<>();
    partSpec.forEach((k, v) -> {
      DataType fieldType = fieldTypes.get(fieldNames.indexOf(k));
      if (!DataTypeUtils.isDatetimeType(fieldType)) {
        // date time type partition field is formatted specifically,
        // read directly from the data file to avoid format mismatch or precision loss
        partObjects.put(k, DataTypeUtils.resolvePartition(defaultPartName.equals(v) ? null : v, fieldType));
      }
    });

    return ParquetSplitReaderUtil.genPartColumnarRowReader(
        this.conf.getBoolean(FlinkOptions.UTC_TIMEZONE),
        true,
        HadoopConfigurations.getParquetConf(this.conf, hadoopConf),
        actualFieldNames.toArray(new String[0]),
        actualFieldTypes.toArray(new DataType[0]),
        partObjects,
        requiredPos,
        2048,
        new org.apache.flink.core.fs.Path(path),
        0,
        Long.MAX_VALUE); // read the whole file
  }

  private ClosableIterator<RowData> getLogFileIterator(MergeOnReadInputSplit split) {
    final Schema tableSchema = new Schema.Parser().parse(tableState.getAvroSchema());
//    final Schema tableSchema = AvroInternalSchemaConverter.convert(querySchema, new Schema.Parser().parse(tableState.getAvroSchema()).getName());
    final Schema requiredSchema = new Schema.Parser().parse(tableState.getRequiredAvroSchema());
    final GenericRecordBuilder recordBuilder = new GenericRecordBuilder(requiredSchema);
    final AvroToRowDataConverters.AvroToRowDataConverter avroToRowDataConverter =
        AvroToRowDataConverters.createRowConverter(tableState.getRequiredRowType());
    final HoodieMergedLogRecordScanner scanner = FormatUtils.logScanner(split, querySchema, conf, hadoopConf);
    final Iterator<String> logRecordsKeyIterator = scanner.getRecords().keySet().iterator();
    final int[] pkOffset = tableState.getPkOffsetsInRequired();
    // flag saying whether the pk semantics has been dropped by user specified
    // projections. For e.g, if the pk fields are [a, b] but user only select a,
    // then the pk semantics is lost.
    final boolean pkSemanticLost = Arrays.stream(pkOffset).anyMatch(offset -> offset == -1);
    final LogicalType[] pkTypes = pkSemanticLost ? null : tableState.getPkTypes(pkOffset);
    final StringToRowDataConverter converter = pkSemanticLost ? null : new StringToRowDataConverter(pkTypes);

    return new ClosableIterator<RowData>() {
      private RowData currentRecord;

      @Override
      public boolean hasNext() {
        while (logRecordsKeyIterator.hasNext()) {
          String curAvroKey = logRecordsKeyIterator.next();
          Option<IndexedRecord> curAvroRecord = null;
          final HoodieAvroRecord<?> hoodieRecord = (HoodieAvroRecord) scanner.getRecords().get(curAvroKey);
          try {
            curAvroRecord = hoodieRecord.getData().getInsertValue(tableSchema);
          } catch (IOException e) {
            throw new HoodieException("Get avro insert value error for key: " + curAvroKey, e);
          }
          if (!curAvroRecord.isPresent()) {
            // delete record found
            if (emitDelete && !pkSemanticLost) {
              GenericRowData delete = new GenericRowData(tableState.getRequiredRowType().getFieldCount());

              final String recordKey = hoodieRecord.getRecordKey();
              final String[] pkFields = KeyGenUtils.extractRecordKeys(recordKey);
              final Object[] converted = converter.convert(pkFields);
              for (int i = 0; i < pkOffset.length; i++) {
                delete.setField(pkOffset[i], converted[i]);
              }
              delete.setRowKind(RowKind.DELETE);

              this.currentRecord = delete;
              return true;
            }
            // skipping if the condition is unsatisfied
            // continue;
          } else {
            final IndexedRecord avroRecord = curAvroRecord.get();
            final RowKind rowKind = FormatUtils.getRowKindSafely(avroRecord, tableState.getOperationPos());
            if (rowKind == RowKind.DELETE && !emitDelete) {
              // skip the delete record
              continue;
            }
            GenericRecord requiredAvroRecord = buildAvroRecordBySchema(
                avroRecord,
                requiredSchema,
                requiredPos,
                recordBuilder);
            currentRecord = (RowData) avroToRowDataConverter.convert(requiredAvroRecord);
            currentRecord.setRowKind(rowKind);
            return true;
          }
        }
        return false;
      }

      @Override
      public RowData next() {
        return currentRecord;
      }

      @Override
      public void close() {
        scanner.close();
      }
    };
  }

  private ClosableIterator<RowData> getUnMergedLogFileIterator(MergeOnReadInputSplit split) {
    final Schema tableSchema = new Schema.Parser().parse(tableState.getAvroSchema());
    final Schema requiredSchema = new Schema.Parser().parse(tableState.getRequiredAvroSchema());
    final GenericRecordBuilder recordBuilder = new GenericRecordBuilder(requiredSchema);
    final AvroToRowDataConverters.AvroToRowDataConverter avroToRowDataConverter =
        AvroToRowDataConverters.createRowConverter(tableState.getRequiredRowType());
    final FormatUtils.BoundedMemoryRecords records = new FormatUtils.BoundedMemoryRecords(split, querySchema, hadoopConf, conf);
    final Iterator<HoodieRecord<?>> recordsIterator = records.getRecordsIterator();

    return new ClosableIterator<RowData>() {
      private RowData currentRecord;

      @Override
      public boolean hasNext() {
        while (recordsIterator.hasNext()) {
          Option<IndexedRecord> curAvroRecord = null;
          final HoodieAvroRecord<?> hoodieRecord = (HoodieAvroRecord) recordsIterator.next();
          try {
            curAvroRecord = hoodieRecord.getData().getInsertValue(tableSchema);
          } catch (IOException e) {
            throw new HoodieException("Get avro insert value error for key: " + hoodieRecord.getRecordKey(), e);
          }
          if (curAvroRecord.isPresent()) {
            final IndexedRecord avroRecord = curAvroRecord.get();
            GenericRecord requiredAvroRecord = buildAvroRecordBySchema(
                avroRecord,
                requiredSchema,
                requiredPos,
                recordBuilder);
            currentRecord = (RowData) avroToRowDataConverter.convert(requiredAvroRecord);
            FormatUtils.setRowKind(currentRecord, avroRecord, tableState.getOperationPos());
            return true;
          }
        }
        return false;
      }

      @Override
      public RowData next() {
        return currentRecord;
      }

      @Override
      public void close() {
        records.close();
      }
    };
  }

  // -------------------------------------------------------------------------
  //  Inner Class
  // -------------------------------------------------------------------------

  static class BaseFileOnlyIterator implements RecordIterator {
    // base file reader
    private final ParquetColumnarRowSplitReader reader;

    BaseFileOnlyIterator(ParquetColumnarRowSplitReader reader) {
      this.reader = reader;
    }

    @Override
    public boolean reachedEnd() throws IOException {
      return this.reader.reachedEnd();
    }

    @Override
    public RowData nextRecord() {
      return this.reader.nextRecord();
    }

    @Override
    public void close() throws IOException {
      if (this.reader != null) {
        this.reader.close();
      }
    }
  }

  /**
   * Similar with {@link BaseFileOnlyIterator} but with instant time filtering.
   */
  static class BaseFileOnlyFilteringIterator implements RecordIterator {
    // base file reader
    private final ParquetColumnarRowSplitReader reader;
    private final InstantRange instantRange;

    private RowData currentRecord;

    BaseFileOnlyFilteringIterator(InstantRange instantRange, ParquetColumnarRowSplitReader reader) {
      this.instantRange = instantRange;
      this.reader = reader;
    }

    @Override
    public boolean reachedEnd() throws IOException {
      while (!this.reader.reachedEnd()) {
        currentRecord = this.reader.nextRecord();
        boolean isInRange = instantRange.isInRange(currentRecord.getString(HOODIE_COMMIT_TIME_COL_POS).toString());
        if (isInRange) {
          return false;
        }
      }
      return true;
    }

    @Override
    public RowData nextRecord() {
      return currentRecord;
    }

    @Override
    public void close() throws IOException {
      if (this.reader != null) {
        this.reader.close();
      }
    }
  }

  static class LogFileOnlyIterator implements RecordIterator {
    // iterator for log files
    private final ClosableIterator<RowData> iterator;

    LogFileOnlyIterator(ClosableIterator<RowData> iterator) {
      this.iterator = iterator;
    }

    @Override
    public boolean reachedEnd() {
      return !this.iterator.hasNext();
    }

    @Override
    public RowData nextRecord() {
      return this.iterator.next();
    }

    @Override
    public void close() {
      if (this.iterator != null) {
        this.iterator.close();
      }
    }
  }

  static class SkipMergeIterator implements RecordIterator {
    // base file reader
    private final RecordIterator baseIterator;
    // iterator for log files
    private final ClosableIterator<RowData> logsIterator;

    // add the flag because the flink ParquetColumnarRowSplitReader is buggy:
    // method #reachedEnd() returns false after it returns true.
    // refactor it out once FLINK-22370 is resolved.
    private boolean readLogs = false;

    private RowData currentRecord;

    SkipMergeIterator(RecordIterator baseIterator, ClosableIterator<RowData> logsIterator) {
      this.baseIterator = baseIterator;
      this.logsIterator = logsIterator;
    }

    @Override
    public boolean reachedEnd() throws IOException {
      if (!readLogs && !this.baseIterator.reachedEnd()) {
        currentRecord = this.baseIterator.nextRecord();
        return false;
      }
      readLogs = true;
      if (this.logsIterator.hasNext()) {
        currentRecord = this.logsIterator.next();
        return false;
      }
      return true;
    }

    @Override
    public RowData nextRecord() {
      return currentRecord;
    }

    @Override
    public void close() throws IOException {
      if (this.baseIterator != null) {
        this.baseIterator.close();
      }
      if (this.logsIterator != null) {
        this.logsIterator.close();
      }
    }
  }

  static class MergeIterator implements RecordIterator {
    // base file reader
    private final ParquetColumnarRowSplitReader reader;
    // log keys used for merging
    private final Iterator<String> logKeysIterator;
    // scanner
    private final HoodieMergedLogRecordScanner scanner;

    private final Schema tableSchema;
    private final Schema requiredSchema;
    private final int[] requiredPos;
    private final boolean emitDelete;
    private final int operationPos;
    private final RowDataToAvroConverters.RowDataToAvroConverter rowDataToAvroConverter;
    private final AvroToRowDataConverters.AvroToRowDataConverter avroToRowDataConverter;
    private final GenericRecordBuilder recordBuilder;

    private final RowDataProjection projection;
    private final Option<RowDataProjection> projectionBeforeMerge;

    private final InstantRange instantRange;

    // add the flag because the flink ParquetColumnarRowSplitReader is buggy:
    // method #reachedEnd() returns false after it returns true.
    // refactor it out once FLINK-22370 is resolved.
    private boolean readLogs = false;

    private final Set<String> keyToSkip = new HashSet<>();

    private final Properties payloadProps;

    private RowData currentRecord;

    MergeIterator(
        Configuration flinkConf,
        org.apache.hadoop.conf.Configuration hadoopConf,
        MergeOnReadInputSplit split,
        RowType tableRowType,
        RowType requiredRowType,
        Schema tableSchema,
        Schema requiredSchema,
        InternalSchema internalSchema,
        RowDataProjection projection,
        Option<RowDataProjection> projectionBeforeMerge,
        int[] requiredPos,
        boolean emitDelete,
        int operationPos,
        ParquetColumnarRowSplitReader reader) { // the reader should be with full schema
      this.tableSchema = tableSchema;
      this.reader = reader;
      this.payloadProps = StreamerUtil.getPayloadConfig(flinkConf).getProps();
      this.scanner = FormatUtils.logScanner(split, internalSchema, flinkConf, hadoopConf);
      this.logKeysIterator = scanner.getRecords().keySet().iterator();
      this.requiredSchema = requiredSchema;
      this.requiredPos = requiredPos;
      this.emitDelete = emitDelete;
      this.operationPos = operationPos;
      this.recordBuilder = new GenericRecordBuilder(requiredSchema);
      this.rowDataToAvroConverter = RowDataToAvroConverters.createConverter(tableRowType);
      this.avroToRowDataConverter = AvroToRowDataConverters.createRowConverter(requiredRowType);
      this.projection = projection;
      this.projectionBeforeMerge = projectionBeforeMerge;
      this.instantRange = split.getInstantRange().orElse(null);
    }

    @Override
    public boolean reachedEnd() throws IOException {
      while (!readLogs && !this.reader.reachedEnd()) {
        currentRecord = this.reader.nextRecord();
        if (instantRange != null) {
          boolean isInRange = instantRange.isInRange(currentRecord.getString(HOODIE_COMMIT_TIME_COL_POS).toString());
          if (!isInRange) {
            // filter base file by instant range
            continue;
          }
        }
        final String curKey = currentRecord.getString(HOODIE_RECORD_KEY_COL_POS).toString();
        if (scanner.getRecords().containsKey(curKey)) {
          keyToSkip.add(curKey);
          currentRecord = projectionBeforeMerge.map(pr -> pr.project(currentRecord)).orElse(currentRecord);
          Option<IndexedRecord> mergedAvroRecord = mergeRowWithLog(currentRecord, curKey);
          if (!mergedAvroRecord.isPresent()) {
            // deleted
            continue;
          } else {
            final RowKind rowKind = FormatUtils.getRowKindSafely(mergedAvroRecord.get(), this.operationPos);
            if (!emitDelete && rowKind == RowKind.DELETE) {
              // deleted
              continue;
            }
            GenericRecord avroRecord = buildAvroRecordBySchema(
                mergedAvroRecord.get(),
                requiredSchema,
                requiredPos,
                recordBuilder);
            this.currentRecord = (RowData) avroToRowDataConverter.convert(avroRecord);
            this.currentRecord.setRowKind(rowKind);
            return false;
          }
        }
        // project the full record in base with required positions
        currentRecord = projection.project(currentRecord);
        return false;
      }
      // read the logs
      readLogs = true;
      while (logKeysIterator.hasNext()) {
        final String curKey = logKeysIterator.next();
        if (!keyToSkip.contains(curKey)) {
          Option<IndexedRecord> insertAvroRecord = getInsertValue(curKey);
          if (insertAvroRecord.isPresent()) {
            // the record is a DELETE if insertAvroRecord not present, skipping
            GenericRecord avroRecord = buildAvroRecordBySchema(
                insertAvroRecord.get(),
                requiredSchema,
                requiredPos,
                recordBuilder);
            this.currentRecord = (RowData) avroToRowDataConverter.convert(avroRecord);
            FormatUtils.setRowKind(this.currentRecord, insertAvroRecord.get(), this.operationPos);
            return false;
          }
        }
      }
      return true;
    }

    private Option<IndexedRecord> getInsertValue(String curKey) throws IOException {
      final HoodieAvroRecord<?> record = (HoodieAvroRecord) scanner.getRecords().get(curKey);
      if (!emitDelete && HoodieOperation.isDelete(record.getOperation())) {
        return Option.empty();
      }
      return record.getData().getInsertValue(tableSchema);
    }

    @Override
    public RowData nextRecord() {
      return currentRecord;
    }

    @Override
    public void close() throws IOException {
      if (this.reader != null) {
        this.reader.close();
      }
      if (this.scanner != null) {
        this.scanner.close();
      }
    }

    private Option<IndexedRecord> mergeRowWithLog(
        RowData curRow,
        String curKey) throws IOException {
      final HoodieAvroRecord<?> record = (HoodieAvroRecord) scanner.getRecords().get(curKey);
      GenericRecord historyAvroRecord = (GenericRecord) rowDataToAvroConverter.convert(tableSchema, curRow);
      return record.getData().combineAndGetUpdateValue(historyAvroRecord, tableSchema, payloadProps);
    }
  }

  // -------------------------------------------------------------------------
  //  Utilities
  // -------------------------------------------------------------------------

  private static int[] getRequiredPosWithCommitTime(int[] requiredPos) {
    int[] requiredPos2 = new int[requiredPos.length + 1];
    requiredPos2[0] = HOODIE_COMMIT_TIME_COL_POS;
    System.arraycopy(requiredPos, 0, requiredPos2, 1, requiredPos.length);
    return requiredPos2;
  }

  @VisibleForTesting
  public void isEmitDelete(boolean emitDelete) {
    this.emitDelete = emitDelete;
  }

  private void projectRecordIterator() {
    getCastProjection().ifPresent(this::projectRecordIterator);
  }

  private void projectRecordIterator(RowDataProjection projection) {
    this.iterator = new RecordIterator.ProjectionIterator(this.iterator, projection);
  }

  private Option<RowDataProjection> getCastProjection() {
    return getCastProjection(IntStream.range(0, requiredPos.length).toArray());
  }

  private Option<RowDataProjection> getCastProjection(int[] positions) {
    if (schemaEvolutionContext.isPresent()) {
      CastMap castMap = schemaEvolutionContext.get().getCastMap(querySchema, actualSchema);
      if (castMap.containsAnyPos(requiredPos)) {
        return Option.of(new RowDataCastProjection(
            SchemaEvolutionContext.project(actualFieldTypes, requiredPos),
            positions,
            castMap.withNewPositions(requiredPos, IntStream.range(0, requiredPos.length).toArray())));
      }
    }
    return Option.empty();
  }
}
