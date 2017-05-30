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

package org.apache.drill.exec.store.elasticsearch;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Charsets;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.collections.MapUtils;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.MajorType;
import org.apache.drill.common.types.SchemaTypeProtos;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.expr.holders.BigIntHolder;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.store.AbstractRecordReader;
import org.apache.drill.exec.store.elasticsearch.internal.ElasticSearchCursor;
import org.apache.drill.exec.vector.BaseValueVector;
import org.apache.drill.exec.vector.complex.fn.JsonReader;
import org.apache.drill.exec.vector.complex.impl.VectorContainerWriter;
import org.omg.CORBA.StringHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

//TODO
public class ElasticSearchRecordReader extends AbstractRecordReader {

    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchRecordReader.class);
    private final ElasticSearchStoragePlugin plugin;
    private final FragmentContext fragmentContext;
    private final boolean unionEnabled;
    private final ElasticSearchScanSpec scanSpec;
    private final Boolean enableAllTextMode;
    private final Boolean readNumbersAsDouble;
    private Set<String> fields;
    private OperatorContext operatorContext;
    private VectorContainerWriter writer;
    private ElasticSearchCursor cursor;
    private JsonReader jsonReader;
    private OutputMutator output;

    public ElasticSearchRecordReader(ElasticSearchScanSpec elasticSearchScanSpec, List<SchemaPath> columns,
                                     FragmentContext context, ElasticSearchStoragePlugin elasticSearchStoragePlugin) {
        //TODO
        this.fields = new HashSet<>();
        this.plugin = elasticSearchStoragePlugin;
        this.fragmentContext = context;
        this.scanSpec = elasticSearchScanSpec;
        setColumns(columns);
        //TODO: What does this mean?
        this.unionEnabled = fragmentContext.getOptions().getOption(ExecConstants.ENABLE_UNION_TYPE);
        //TODO: These should be place out of Mongo attributes
        this.enableAllTextMode = fragmentContext.getOptions().getOption(ExecConstants.MONGO_ALL_TEXT_MODE).bool_val;
        this.readNumbersAsDouble = fragmentContext.getOptions().getOption(ExecConstants.MONGO_READER_READ_NUMBERS_AS_DOUBLE).bool_val;
    }

    @Override
    protected Collection<SchemaPath> transformColumns(Collection<SchemaPath> projectedColumns) {
        Set<SchemaPath> transformed = Sets.newLinkedHashSet();
        //TODO: See if we can only poll for selected columns
        if (!isStarQuery()) {
            for (SchemaPath column : projectedColumns) {
                String fieldName = column.getRootSegment().getPath();
                transformed.add(column);
                this.fields.add(fieldName);
            }
        } else {
            transformed.add(AbstractRecordReader.STAR_COLUMN);
        }
        return transformed;
    }

    @Override
    public void setup(OperatorContext context, OutputMutator output) throws ExecutionSetupException {
        this.operatorContext = context;
        this.output = output;
        this.writer = new VectorContainerWriter(output,this.unionEnabled);
        this.jsonReader = new JsonReader(fragmentContext.getManagedBuffer(), Lists.newArrayList(getColumns()),
                enableAllTextMode, false, readNumbersAsDouble);
    }

    @Override
    public int next() {
        if (this.cursor == null) {
            logger.info("Initializing cursor");
            try {
                this.cursor = ElasticSearchCursor.scroll(this.plugin.getClient(), this.plugin.getObjectMapper(),
                        this.scanSpec.getIndexName(), this.scanSpec.getTypeMappingName(), MapUtils.EMPTY_MAP, null);
            } catch (IOException e) {
                throw new DrillRuntimeException("Error while initializing cursor", e);
            }
        }
        writer.allocate();
        writer.reset();

        int docCount = 0;
        Stopwatch watch = Stopwatch.createStarted();

        try {
            while (docCount < BaseValueVector.INITIAL_VALUE_ALLOCATION && cursor.hasNext()) {
                writer.setPosition(docCount);
                JsonNode element = cursor.next();
                JsonNode id = JsonHelper.getPath(element, "_id");
                //HACK: This is done so we can poll _id from elastic into object content
                ObjectNode content = (ObjectNode) JsonHelper.getPath(element, "_source");
                content.put("_id",id.asText());
                this.jsonReader.setSource(content);;
                this.jsonReader.write(writer);
                docCount++;
            }
            this.jsonReader.ensureAtLeastOneField(writer);
            writer.setValueCount(docCount);
            logger.debug("Took {} ms to get {} records", watch.elapsed(TimeUnit.MILLISECONDS), docCount);
            return docCount;
        } catch (IOException e) {
            String msg = "Failure while reading document. - Parser was at record: " + (docCount + 1);
            logger.error(msg, e);
            throw new DrillRuntimeException(msg, e);
        }
    }

    @Override
    public void close() throws Exception {

    }
}
