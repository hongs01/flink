/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.elasticsearch7;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.connectors.elasticsearch.IndexManagerBase;

import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;

import java.io.IOException;
import java.util.PriorityQueue;

/**
 * Index manager for Elasticsearch version 7.
 *
 * <p>Note: This class is binary compatible to Elasticsearch 7.
 */
@Internal
public class Elasticsearch7IndexManager extends IndexManagerBase {

	private final RestHighLevelClient client;
	private final PriorityQueue<String> indexCache;
	private static final int DEFAULT_CACHE_SIZE = 24;

	public Elasticsearch7IndexManager(RestHighLevelClient client) {
		this.client = client;
		this.indexCache = new PriorityQueue<>(DEFAULT_CACHE_SIZE);
	}

	protected boolean existsInCache(String index) {
		return this.indexCache.contains(index);
	}

	protected void updateCache(String index) {
		if (this.indexCache.size() > DEFAULT_CACHE_SIZE) {
			this.indexCache.poll();
		}
		this.indexCache.add(index);
	}

	protected boolean indexExists(String index) throws IOException {
		GetIndexRequest req = new GetIndexRequest(index);
		return client.indices().exists(req, RequestOptions.DEFAULT);
	}

	protected boolean createIndex(String index) throws IOException {
		CreateIndexRequest req = new CreateIndexRequest(index);
		CreateIndexResponse resp =  client.indices().create(req, RequestOptions.DEFAULT);
		return resp.isAcknowledged() || indexExists(index);
	}

	protected boolean aliasExists(String index, String alias) throws IOException {
		GetAliasesRequest req = new GetAliasesRequest().aliases(alias).indices(index);
		return client.indices().existsAlias(req, RequestOptions.DEFAULT);
	}

	protected boolean createAlias(String index, String alias) throws IOException {
		IndicesAliasesRequest req = new IndicesAliasesRequest().addAliasAction(IndicesAliasesRequest
				.AliasActions.add()
				.index(index)
				.alias(alias));
		AcknowledgedResponse resp = client.indices().updateAliases(req, RequestOptions.DEFAULT);
		return resp.isAcknowledged() || aliasExists(index, alias);
	}
}
