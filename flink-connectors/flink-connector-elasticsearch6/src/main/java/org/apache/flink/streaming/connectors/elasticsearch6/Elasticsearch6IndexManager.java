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

package org.apache.flink.streaming.connectors.elasticsearch6;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.connectors.elasticsearch.IndexManagerBase;

import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RestHighLevelClient;

import java.util.PriorityQueue;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Index manager for Elasticsearch version 6.
 *
 * <p>Note: This class is binary compatible to Elasticsearch 6.
 */
@Internal
public class Elasticsearch6IndexManager extends IndexManagerBase {

	private final RestHighLevelClient client;
	private final PriorityQueue<String> indexCache;
	private static final int DEFAULT_CACHE_SIZE = 24;

	public Elasticsearch6IndexManager(RestHighLevelClient client) {
		this.client = checkNotNull(client);
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
		GetIndexRequest req = new GetIndexRequest().indices(index);
		return client.indices().exists(req);
	}

	protected boolean createIndex(String index) throws Exception {
		CreateIndexRequest req = new CreateIndexRequest(index);
		CreateIndexResponse resp =  client.indices().create(req);
		return resp.isAcknowledged() || indexExists(index);
	}

	protected boolean aliasExists(String index, String alias) throws Exception {
		GetAliasesRequest req = new GetAliasesRequest().aliases(alias).indices(index);
		return client.indices().existsAlias(req);
	}

	protected boolean createAlias(String index, String alias) throws Exception {
		IndicesAliasesRequest req = new IndicesAliasesRequest().addAliasAction(IndicesAliasesRequest
			.AliasActions.add()
			.index(index)
			.alias(alias));
		AcknowledgedResponse resp = client.indices().updateAliases(req);
		return resp.isAcknowledged() || aliasExists(index, alias);
	}
}
