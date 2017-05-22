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

package org.apache.drill.store.elasticsearch;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.PhysicalOperatorSetupException;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.SubScan;
import org.apache.drill.exec.proto.CoordinationProtos;
import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

@JsonTypeName("elasticsearch-scan")
public class ElasticSearchGroupScan extends AbstractGroupScan {

    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchGroupScan.class);

    private final ElasticSearchStoragePlugin storagePlugin;
    private final ElasticSearchPluginConfig storagePluginConfig;
    private final ElasticSearchScanSpec scanSpec;
    private final List<SchemaPath> columns;
    private Stopwatch watch;

    @JsonCreator
    public ElasticSearchGroupScan(
            @JsonProperty("usernName") String userName,
            @JsonProperty("elasticSearchSpec") ElasticSearchScanSpec scanSpec,
            @JsonProperty("storage") ElasticSearchPluginConfig storagePluginConfig,
            @JsonProperty("columns") List<SchemaPath> columns,
            @JacksonInject StoragePluginRegistry pluginRegistry) throws IOException, ExecutionSetupException {
        this(userName, (ElasticSearchStoragePlugin) pluginRegistry.getPlugin(storagePluginConfig), scanSpec, columns);
    }

    public ElasticSearchGroupScan(String userName, ElasticSearchStoragePlugin plugin, ElasticSearchScanSpec scanSpec, List<SchemaPath> columns) {
        super(userName);
        this.storagePlugin = plugin;
        this.storagePluginConfig = plugin.getConfig();
        this.scanSpec = scanSpec;
        this.columns = columns;
        this.watch  = Stopwatch.createUnstarted();
        init();
    }

    public ElasticSearchGroupScan(ElasticSearchGroupScan that) {
        this(that.getUserName(), that.storagePlugin, that.scanSpec, that.columns);
    }

    private void init() {
        //TODO: init whatever
    }

    @Override
    public void applyAssignments(List<CoordinationProtos.DrillbitEndpoint> endpoints) throws PhysicalOperatorSetupException {
        logger.debug("Incoming endpoints :" + endpoints);
        this.watch.reset();
        this.watch.start();

        this.watch.stop();

    }

    @Override
    public SubScan getSpecificScan(int minorFragmentId) throws ExecutionSetupException {
        // TODO: What is minor fragmentation id ?
        return new ElasticSearchSubScan(super.getUserName(), this.storagePlugin, this.storagePluginConfig, this.scanSpec, this.columns);
    }

    @Override
    public int getMaxParallelizationWidth() {
        return 1;
    }

    @Override
    public String getDigest() {
        return toString();
    }

    @Override
    public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) throws ExecutionSetupException {
        Preconditions.checkArgument(children.isEmpty());
        return new ElasticSearchGroupScan(this);
    }
}
