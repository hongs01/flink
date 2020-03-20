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

package org.apache.flink.streaming.connectors.elasticsearch;

import org.apache.flink.annotation.Internal;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesResponse;
import org.elasticsearch.action.admin.indices.alias.exists.AliasesExistResponse;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.client.transport.TransportClient;

import java.util.PriorityQueue;

/**
 * Index manager for Elasticsearch previous version 6.
 */
@Internal
public class PreElasitcsearch6IndexManager extends IndexManagerBase {

	private final TransportClient client;
	private final PriorityQueue<String> indexCache;
	private static final int DEFAULT_CACHE_SIZE = 24;

	public PreElasitcsearch6IndexManager(TransportClient client) {
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

	protected boolean indexExists(String index) throws Exception {
		IndicesExistsRequest req = new IndicesExistsRequest(index);
		ActionFuture<IndicesExistsResponse> actionFuture =  client.admin().indices().exists(req);
		IndicesExistsResponse resp = actionFuture.get();
		return resp.isExists();
	}

	protected boolean createIndex(String index) throws Exception {
		CreateIndexRequest req = new CreateIndexRequest(index);
		ActionFuture<CreateIndexResponse> actionFuture =  client.admin().indices().create(req);
		CreateIndexResponse resp = actionFuture.get();
		return resp.isAcknowledged() || indexExists(index);
	}

	protected boolean aliasExists(String index, String alias) throws Exception {
		GetAliasesRequest req = new GetAliasesRequest().aliases(alias).indices(index);
		ActionFuture<AliasesExistResponse> actionFuture = client.admin().indices().aliasesExist(req);
		AliasesExistResponse resp = actionFuture.get();
		return resp.exists();
	}

	protected boolean createAlias(String index, String alias) throws Exception {
		IndicesAliasesRequest req = new IndicesAliasesRequest().addAliasAction(IndicesAliasesRequest
			.AliasActions.add()
			.index(index)
			.alias(alias));
		ActionFuture<IndicesAliasesResponse>  actionFuture = client.admin().indices().aliases(req);
		IndicesAliasesResponse resp = actionFuture.get();
		return resp.isAcknowledged() || aliasExists(index, alias);
	}
}
