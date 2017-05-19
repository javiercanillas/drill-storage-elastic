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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.drill.common.logical.StoragePluginConfigBase;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.elasticsearch.xpack.client.PreBuiltXPackTransportClient;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;

@JsonTypeName(ElasticSearchPluginConfig.NAME)
public class ElasticSearchPluginConfig extends StoragePluginConfigBase {
    static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ElasticSearchPluginConfig.class);

    public static final String NAME = "elasticsearch";

    private final int port;
    private final Collection<String> hosts;
    private final String credentials;
    private final String clusterName;
    private final Integer hashCode;

    public ElasticSearchPluginConfig(@JsonProperty(value = "port", defaultValue = "9300") int port,
                                     @JsonProperty(value = "credentials", defaultValue = "") String credentials,
                                     @JsonProperty(value = "hosts", required = true) String hosts,
                                     @JsonProperty(value = "clustername", defaultValue = "elasticsearch") String clustername) {
        this.port = port;
        if (!StringUtils.isEmpty(credentials)) {
            this.credentials = credentials;
        } else {
            this.credentials = null;
        }
        List<String> hostList = Arrays.asList(hosts.split(","));
        this.hosts = hostList;
        this.clusterName = clustername;

        // Building hashcode
        HashCodeBuilder builder = new HashCodeBuilder(13,7);

        builder.append(this.port)
                .append(this.credentials)
                .append(this.clusterName)
                .append(this.hosts);
        Collections.sort(hostList);
        for(String host : hostList) {
            builder.append(host);
        }
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
        return this.port == thatConfig.port
                && this.hosts.size() == thatConfig.hosts.size() && this.hosts.containsAll(thatConfig.hosts)
                && (this.credentials == null ? thatConfig.credentials == null : this.credentials.equals(thatConfig.credentials))
                && this.clusterName.equals(thatConfig.clusterName);
    }

    @Override
    public int hashCode() {
        return this.hashCode;
    }

    @JsonIgnore
    public TransportClient createClient() throws UnknownHostException {
        Settings.Builder settingsBuilder = Settings.builder()
                .put("cluster.name", this.clusterName);

        PreBuiltTransportClient client;
        if (StringUtils.isEmpty(this.credentials)) {
            client = new PreBuiltTransportClient(settingsBuilder.build());
        } else {
            client = new PreBuiltXPackTransportClient(settingsBuilder.put("xpack.security.user", this.credentials).build());
        }
        for(String hosts : this.hosts) {
            client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(hosts), this.port));
        }
        return client;
    }

    public int getPort() {
        return port;
    }

    public Collection<String> getHosts() {
        return hosts;
    }

    public String getCredentials() {
        return credentials;
    }

    public String getClusterName() {
        return clusterName;
    }
}
