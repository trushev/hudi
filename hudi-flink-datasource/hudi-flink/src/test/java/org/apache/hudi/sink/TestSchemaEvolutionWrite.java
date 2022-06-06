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

package org.apache.hudi.sink;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.internal.schema.Types;
import org.apache.hudi.sink.utils.TestWriteBase;
import org.apache.hudi.table.HoodieTableSource;
import org.apache.hudi.util.AvroSchemaConverter;
import org.apache.hudi.utils.TestConfigurations;
import org.apache.hudi.utils.TestData;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.hudi.internal.schema.action.TableChange.ColumnPositionChange.ColumnPositionType.AFTER;
import static org.apache.hudi.utils.TestConfigurations.ROW_DATA_TYPE_EVOLUTION;
import static org.apache.hudi.utils.TestConfigurations.ROW_TYPE_EVOLUTION;
import static org.apache.hudi.utils.TestConfigurations.ROW_TYPE_OPTION;

@Disabled
public class TestSchemaEvolutionWrite extends TestWriteBase {

  @TempDir
  File tempFile;

  @Test
  public void testSchemaEvo() throws Exception {
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    conf.setString(FlinkOptions.TABLE_TYPE, HoodieTableType.COPY_ON_WRITE.name());
    conf.setBoolean(FlinkOptions.SCHEMA_EVOLUTION_ENABLED, true);

    Schema doubleType = SchemaBuilder.unionOf().nullType().and().doubleType().endUnion();
    TestHarness.instance().preparePipeline(tempFile, conf)
            .consume(TestData.DATA_SET_INSERT)
            .assertEmptyDataFiles()
            .checkpoint(1)
            .assertNextEvent()
            .checkpointComplete(1)
            .checkWrittenData(EXPECTED1)
            .addColumn("salary", doubleType, null, "age", AFTER)
            .renameColumn("name", "first_name")
            .updateColumnType("age", Types.StringType.get())
            .end();

    conf.setString(FlinkOptions.SOURCE_AVRO_SCHEMA, AvroSchemaConverter.convertToSchema(ROW_TYPE_EVOLUTION).toString());
    conf.removeConfig(FlinkOptions.SOURCE_AVRO_SCHEMA_PATH);
    conf.setString(ROW_TYPE_OPTION, ROW_TYPE_EVOLUTION.asSerializableString());

    TestHarness.instance().preparePipeline(tempFile, conf)
            .consume(TestData.DATA_SET_EVOLUTION)
            .checkpoint(1)
            .assertNextEvent()
            .checkpointComplete(1)
            .checkWrittenData(EXPECTED6)
            .end();

    ResolvedSchema resolvedSchema = org.apache.hudi.utils.SchemaBuilder.instance()
            .fields(ROW_TYPE_EVOLUTION.getFieldNames(), ROW_DATA_TYPE_EVOLUTION.getChildren())
            .primaryKey("uuid")
            .build();
    HoodieTableSource tableSource = new HoodieTableSource(
            resolvedSchema,
            new Path(tempFile.getAbsolutePath()),
            Collections.singletonList("partition"),
            "default",
            conf);

    @SuppressWarnings("unchecked")
    InputFormat<RowData, InputSplit> inputFormat = (InputFormat<RowData, InputSplit>) tableSource.getInputFormat();
    InputSplit[] inputSplits = inputFormat.createInputSplits(1);

    List<RowData> result = new ArrayList<>();
    RowDataSerializer serializer = new RowDataSerializer(ROW_TYPE_EVOLUTION);
    for (InputSplit inputSplit : inputSplits) {
      inputFormat.open(inputSplit);
      while (!inputFormat.reachedEnd()) {
        result.add(serializer.copy(inputFormat.nextRecord(null)));
      }
      inputFormat.close();
    }
    result.forEach(System.out::println);


  }
}
