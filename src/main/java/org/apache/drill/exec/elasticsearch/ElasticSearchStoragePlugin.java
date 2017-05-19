/**
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
package org.apache.drill.exec.elasticsearch;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.drill.common.JSONOptions;
import org.apache.drill.exec.elasticsearch.schema.ElasticSearchSchemaFactory;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.AbstractStoragePlugin;
import org.apache.drill.exec.store.SchemaConfig;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;

import java.io.IOException;

public class ElasticSearchStoragePlugin extends AbstractStoragePlugin {
    static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ElasticSearchStoragePlugin.class);

    private final String name;
    private final DrillbitContext context;
    private final ElasticSearchPluginConfig config;
    private final ElasticSearchSchemaFactory schemaFactory;
    private TransportClient client;

    public ElasticSearchStoragePlugin(ElasticSearchPluginConfig config, DrillbitContext context, String name) throws IOException {
        this.context = context;
        this.name = name;
        this.config = config;

        this.schemaFactory = new ElasticSearchSchemaFactory(this, name);
    }

    @Override
    public ElasticSearchPluginConfig getConfig() {
        return this.config;
    }

    @Override
    public void registerSchemas(SchemaConfig schemaConfig, SchemaPlus parent) throws IOException {
        this.schemaFactory.registerSchemas(schemaConfig, parent);
    }

    @Override
    public boolean supportsRead() {
        return true;
    }

    @Override
    public void start() throws IOException {
        if (this.client != null) {
            this.client = this.config.createClient();
        } else {
            logger.warn("Already started");
        }
    }

    @Override
    public void close() throws IOException {
        if (this.client != null) {
            this.client.close();
            this.client = null;
        } else {
            logger.warn("Not started or already closed");
        }
    }

    @Override
    public AbstractGroupScan getPhysicalScan(String userName, JSONOptions selection) throws IOException {
        ElasticSearchScanSpec elasticSearchScanSpec = selection.getListWith(new ObjectMapper(), new TypeReference<ElasticSearchScanSpec>() {});
        return new ElasticSearchGroupScan(userName, this, elasticSearchScanSpec, null);
    }

    public Client getClient() { return this.client; }

    public ElasticSearchSchemaFactory getSchemaFactory() {
        return this.schemaFactory;
    }
}
