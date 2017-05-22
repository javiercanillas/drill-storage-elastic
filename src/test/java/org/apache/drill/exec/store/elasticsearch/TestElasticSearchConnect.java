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

import com.fasterxml.jackson.databind.ObjectMapper;
import junit.framework.TestCase;
import org.apache.commons.codec.binary.Base64;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

@Ignore("requires remote ElasticSearch server")
public class TestElasticSearchConnect {

    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestElasticSearchConnect.class);
    private static final String HOST = "127.0.0.1";
    private static final String CREDENTIALS = "elastic:elastic";
    private static final int PORT = 9200;
    private static final int MAXRETRYTIMEOUTMILLIS = 1000;
    private static final String PATHPREFIX = null;
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private RestClient createClient(Header[] headers) {
        RestClientBuilder clientBuilder = RestClient.builder(new HttpHost(HOST, PORT, "http"));
        if (headers != null && headers.length > 0) {
            clientBuilder.setDefaultHeaders(headers);
        }
        clientBuilder.setMaxRetryTimeoutMillis(MAXRETRYTIMEOUTMILLIS);
        //clientBuilder.setPathPrefix(PATHPREFIX);
        return clientBuilder.build();
    }

    @Test
    public void TryConnectWithoutCredentials() throws IOException {
        RestClient restClient = this.createClient(null);
        try {
            Response response = restClient.performRequest("GET", "/_nodes");
            TestCase.assertNotNull(response);
        } finally {
            restClient.close();
        }
    }

    @Test
    public void TryConnectWithCredentials() throws IOException {

        List<BasicHeader> headers = Arrays.asList(new BasicHeader("Authorization", "Basic " + Base64.encodeBase64String(CREDENTIALS.getBytes())));
        RestClient restClient = this.createClient((Header[]) headers.toArray());
        try {
            Response response = restClient.performRequest("GET", "/_nodes");
            TestCase.assertNotNull(response);
        } finally {
            restClient.close();
        }
    }
}
