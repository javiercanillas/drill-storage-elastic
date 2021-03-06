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

package org.apache.drill.exec.store.elasticsearch.internal;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.NotImplementedException;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.exec.store.elasticsearch.JsonHelper;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.entity.BasicHttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;

public class ElasticSearchCursor implements Iterator<JsonNode>, Closeable {

    static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ElasticSearchCursor.class);
    private static final String SCROLLDURATION = "1m";
    private static final String SCROLL = "scroll";

    private final ObjectMapper objMapper;
    private final RestClient client;
    private final long totalHits;
    private final String scrollRequest;
    private final Header[] additionalHealders;
    private long position = 0;
    private Iterator<JsonNode> internalIterator;

    public static ElasticSearchCursor scroll(RestClient client, ObjectMapper objMapper, String idxName, String type,
                                             Map<String,String> additionalQueryParams, HttpEntity requestBody,
                                             Header... additionalHeaders) throws IOException {
        Map<String,String> queryParams = new HashMap<>();
        if (!MapUtils.isEmpty(additionalQueryParams)) {
            queryParams.putAll(additionalQueryParams);
        }
        queryParams.put(SCROLL,SCROLLDURATION);
        Response response = client.performRequest("POST", "/" + idxName + "/" + type + "/_search",
                queryParams, requestBody, additionalHeaders);
        JsonNode rootNode = JsonHelper.readRespondeContentAsJsonTree(objMapper, response);
        JsonNode scrollIdNode = JsonHelper.getPath(rootNode, "_scroll_id");
        String scrollId;
        if (!scrollIdNode.isMissingNode()) {
            scrollId = scrollIdNode.asText();
        } else {
            throw new DrillRuntimeException("Couldn't get '"+SCROLL+"' for cursor");
        }

        JsonNode totalHitsNode = JsonHelper.getPath(rootNode, "hits.total");
        long totalHits = 0;
        if (!totalHitsNode.isMissingNode()) {
            totalHits = totalHitsNode.asLong();
        } else {
            throw new DrillRuntimeException("Couldn't get 'hits.total' for cursor");
        }

        JsonNode elementsNode = JsonHelper.getPath(rootNode, "hits.hits");
        Iterator<JsonNode> elementIterator;
        if (!elementsNode.isMissingNode() && elementsNode.isArray()) {
            elementIterator = elementsNode.iterator();
        } else {
            throw new DrillRuntimeException("Couldn't get 'hits.hits' for cursor");
        }
        return new ElasticSearchCursor(client, objMapper, scrollId, totalHits, elementIterator, additionalHeaders);

    }

    private ElasticSearchCursor(RestClient client, ObjectMapper objMapper, String scrollId, long totalHits,
                                Iterator<JsonNode> elementIterator, Header... headers) {
        this.client = client;
        this.objMapper = objMapper;
        this.totalHits = totalHits;
        this.internalIterator = elementIterator;
        this.additionalHealders = headers;
        this.scrollRequest = "{ \"" + SCROLL + "\" : \"" + SCROLLDURATION + "\", \"scroll_id\" : \""+scrollId+"\" }";
    }


    @Override
    public boolean hasNext() {
        return (this.position < this.totalHits);
    }

    @Override
    public JsonNode next() {
        //TODO: Code here
        JsonNode next;
        if (this.hasNext()) {
            if (!this.internalIterator.hasNext()) {
                logger.debug("Internal storage depleted, lets scroll for more");
                try {
                    Response response = this.client.performRequest("POST", "/_search/scroll", MapUtils.EMPTY_MAP,
                            new NStringEntity(this.scrollRequest, ContentType.APPLICATION_JSON), this.additionalHealders);

                    JsonNode rootNode = JsonHelper.readRespondeContentAsJsonTree(objMapper, response);
                    JsonNode elementsNode = JsonHelper.getPath(rootNode, "hits.hits");
                    if (!elementsNode.isMissingNode() && elementsNode.isArray()) {
                        this.internalIterator = elementsNode.iterator();
                    } else {
                        throw new DrillRuntimeException("Couldn't get 'hits.hits' for cursor");
                    }
                } catch (IOException e) {
                    throw new DrillRuntimeException("Couldn't get more elements", e);
                }
            }
        } else {
            throw new NoSuchElementException();
        }
        this.position++;
        return internalIterator.next();
    }

    @Override
    public void remove() {
        throw new NotImplementedException();
    }

    @Override
    public void close() throws IOException {
        //DO NOTHING
    }
}
