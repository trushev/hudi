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

package org.apache.hudi.table.format;

import org.apache.flink.configuration.Configuration;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.util.InternalSchemaCache;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.internal.schema.action.InternalSchemaMerger;
import org.apache.hudi.internal.schema.convert.AvroInternalSchemaConverter;
import org.apache.hudi.util.StreamerUtil;

import javax.annotation.Nullable;
import java.io.Serializable;

public final class SchemaEvolutionContext implements Serializable {
  private final Configuration conf;
  private final boolean isEnabled;
  @Nullable  private final HoodieTableMetaClient metaClient;
  @Nullable  private final InternalSchema querySchema;

  public SchemaEvolutionContext(Configuration conf) {
    this.conf = conf;
    this.isEnabled = conf.getBoolean(FlinkOptions.SCHEMA_EVOLUTION_ENABLED);
    if (isEnabled) {
      this.metaClient = StreamerUtil.createMetaClient(conf);
      this.querySchema = new TableSchemaResolver(metaClient).getTableInternalSchemaFromCommitMetadata().get();
    } else {
      this.metaClient = null;
      this.querySchema = null;
    }
  }

  public InternalSchema getQuerySchema() {
    return querySchema;
  }

  public InternalSchema getInternalSchema() {
    return InternalSchemaCache.searchSchemaAndCache(Long.MAX_VALUE, metaClient, false);
  }

  public InternalSchema getMergedSchema(long commitTime) {
    InternalSchema fileSchema = InternalSchemaCache.searchSchemaAndCache(commitTime, metaClient, false);
    return new InternalSchemaMerger(fileSchema, querySchema, true, true).mergeSchema();
  }

  public String getTableName() {
    return metaClient.getTableConfig().getTableName();
  }

  public boolean isEnabled() {
    return isEnabled;
  }

  public HoodieTableMetaClient getMetaClient() {
    return metaClient;
  }
}
