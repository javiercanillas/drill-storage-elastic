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

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.store.AbstractRecordReader;
import org.apache.drill.exec.store.RecordReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

//TODO
public class ElasticSearchRecordReader extends AbstractRecordReader {

    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchRecordReader.class);

    public ElasticSearchRecordReader(ElasticSearchScanSpec elasticSearchScanSpec, List<SchemaPath> columns,
                                     FragmentContext context, ElasticSearchStoragePlugin elasticSearchStoragePlugin) {
        //TODO
    }

    @Override
    public void setup(OperatorContext context, OutputMutator output) throws ExecutionSetupException {

    }

    @Override
    public int next() {
        return 0;
    }

    @Override
    public void close() throws Exception {

    }
}
