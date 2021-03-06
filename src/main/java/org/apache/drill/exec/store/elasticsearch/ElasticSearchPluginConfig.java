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

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.drill.common.logical.StoragePluginConfigBase;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

/*
import org.elasticsearch.client.transport.TransportClient;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.elasticsearch.xpack.client.PreBuiltXPackTransportClient;
 */

@JsonTypeName(ElasticSearchPluginConfig.NAME)
public class ElasticSearchPluginConfig extends StoragePluginConfigBase {
    public static final String NAME = "elasticsearch";
    static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ElasticSearchPluginConfig.class);
    private static final int DEFAULT_PORT = 9200;
    private static final long DEFAULT_CACHE_DURATION = 5;
    private static final TimeUnit DEFAULT_CACHE_TIMEUNIT = TimeUnit.MINUTES;

    private final String hostsAndPorts;
    private final String credentials;
    private final String pathPrefix;
    private final Integer hashCode;
    private final int maxRetryTimeoutMillis;
    private final long cacheDuration;
    private final TimeUnit cacheTimeUnit;


    /**
     * Create ElasticSearch Plugin configuration
     * @param credentials its format should be "[USERNAME]:[PASSWORD]". For example: 'me:myPassword'. In case of null or empty String, no Authorization will be used
     * @param hostsAndPorts its format is a list of a [PROTOCOL]://[HOST]:[PORT] separated by ','. For example: 'http://localhost:9200,http://localhost:9201'
     */
    public ElasticSearchPluginConfig(@JsonProperty(value = "credentials") String credentials,
                                     @JsonProperty(value = "hostsAndPorts", required = true) String hostsAndPorts,
                                     @JsonProperty(value = "pathPrefix") String pathPrefix,
                                     @JsonProperty(value = "maxRetryTimeoutMillis") int maxRetryTimeoutMillis,
                                     @JsonProperty(value = "cacheDuration") long cacheDuration,
                                     @JsonProperty(value = "cacheTimeUnit") TimeUnit cacheTimeUnit) {
        if (!StringUtils.isEmpty(credentials)) {
            this.credentials = credentials;
        } else {
            this.credentials = null;
        }
        this.hostsAndPorts = hostsAndPorts;
        this.maxRetryTimeoutMillis = maxRetryTimeoutMillis;
        this.pathPrefix = pathPrefix;

        if (cacheDuration > 0) {
            this.cacheDuration = cacheDuration;
        } else {
            this.cacheDuration = DEFAULT_CACHE_DURATION;
        }

        if (cacheTimeUnit != null) {
            this.cacheTimeUnit = cacheTimeUnit;
        } else {
            this.cacheTimeUnit = DEFAULT_CACHE_TIMEUNIT;
        }

        // Building hashcode
        HashCodeBuilder builder = new HashCodeBuilder(13,7);
        builder.append(this.hostsAndPorts)
                .append(this.credentials)
                .append(this.maxRetryTimeoutMillis)
                .append(this.pathPrefix)
                .append(this.cacheDuration)
                .append(this.cacheTimeUnit);
        this.hashCode = builder.build();
    }

    @Override
    public boolean equals(Object that) {
        if (this == that) {
            return true;
        } else if (that == null || getClass() != that.getClass()) {
            return false;
        }
        ElasticSearchPluginConfig thatConfig = (ElasticSearchPluginConfig) that;
        return this.hostsAndPorts.equals(thatConfig.hostsAndPorts)
                && (this.credentials == null ? thatConfig.credentials == null : this.credentials.equals(thatConfig.credentials))
                && this.maxRetryTimeoutMillis == thatConfig.maxRetryTimeoutMillis
                && this.cacheDuration == thatConfig.cacheDuration
                && (this.pathPrefix == null ? thatConfig.pathPrefix == null : this.pathPrefix.equals(thatConfig.pathPrefix))
                && (this.cacheTimeUnit == null ? thatConfig.cacheTimeUnit == null : this.cacheTimeUnit.equals(thatConfig.cacheTimeUnit));
    }

    @Override
    public int hashCode() {
        return this.hashCode;
    }

    public String getHostsAndPorts() {
        return this.hostsAndPorts;
    }

    public String getCredentials() {
        return credentials;
    }

    public String getPathPrefix() {
        return pathPrefix;
    }

    public int getMaxRetryTimeoutMillis() {
        return maxRetryTimeoutMillis;
    }

    /**
     * Creates and returns a client base on configuration
     * @return a {@link RestClient} to work against elasticSearch
     * @throws UnknownHostException
     */
    @JsonIgnore
    public RestClient createClient() {
        RestClientBuilder clientBuilder = RestClient.builder( this.parseHostsAndPorts());

        Header[] headers = this.buildHeaders();
        if (headers != null) {
            clientBuilder.setDefaultHeaders(headers);
        }
        if (this.maxRetryTimeoutMillis > 0) {
            clientBuilder.setMaxRetryTimeoutMillis(this.maxRetryTimeoutMillis);
        }
        if (!StringUtils.isEmpty(this.pathPrefix)) {
            clientBuilder.setPathPrefix(this.pathPrefix);
        }
        return clientBuilder.build();
    }

    private Header[] buildHeaders() {
        Collection<Header> headers = new ArrayList<>();
        if (!StringUtils.isEmpty(this.credentials)) {
            headers.add(new BasicHeader("Authorization", "Basic " + Base64.encodeBase64String(this.credentials.getBytes())));
        }
        return (headers.isEmpty() ? null : (Header[]) headers.toArray());
    }

    private HttpHost[] parseHostsAndPorts() {
        Collection<HttpHost> rtnValue = new ArrayList<>();
        List<String> hostPortList = Arrays.asList(this.hostsAndPorts.split(","));
        for (String hostPort : hostPortList) {
            String[] split = hostPort.split(":");
            String protocol = split[0];
            String host = split[1].replaceAll("/","");
            int port = DEFAULT_PORT;
            if (split.length > 2) {
                port = Integer.parseInt(split[2]);
            }
            rtnValue.add(new HttpHost(host, port, protocol));
        }
        return rtnValue.toArray(new HttpHost[0]);
    }

    public long getCacheDuration() {
        return this.cacheDuration;
    }

    public TimeUnit getCacheTimeUnit() {
        return this.cacheTimeUnit;
    }
}
