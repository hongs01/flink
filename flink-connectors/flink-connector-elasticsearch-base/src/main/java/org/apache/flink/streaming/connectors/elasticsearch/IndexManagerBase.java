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
import org.apache.flink.util.Preconditions;

import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A version-agnostic Elasticsearch index manager.
 *
 * <p>This class manage index and alias across Elasticsearch versions.
 */
@Internal
public abstract class IndexManagerBase implements IndexManager {

	private static final Logger LOG = LoggerFactory.getLogger(IndexManagerBase.class);
	// ignore alias error by default.
	private static final boolean ignoreAliasError = true;

	@Override
	public void addAlias(IndicesAliasesRequest indicesAliasesRequest) {
		for (IndicesAliasesRequest.AliasActions aliasAction : indicesAliasesRequest.getAliasActions()) {
			for (String index : aliasAction.indices()) {
				for (String alias: aliasAction.aliases()) {
					addAliasForIndex(index, alias);
				}
			}
		}
	}

	private void addAliasForIndex(String index, String alias) {
		Preconditions.checkNotNull(index);
		Preconditions.checkNotNull(alias);

		if (existsInCache(index)) {
			return;
		}
		if (index.equals(alias)) {
			if (!ignoreAliasError) {
				throw new RuntimeException(String.format("An alias cannot have the same name as an index in Elasticsearch, " +
					"index: '%s', alias: '%s'", index, alias));
			}
			return;
		}

		final String aliasErrMsg = String.format("Add alias for index fail, index:%s alias:%s cause:", index, alias);
		boolean aliasStatus = false;
		try {
			if (!indexExists(index)) {
				// create index if the index does not exist.
				synchronized (this) {
					try {
						if (createIndex(index)) {
							aliasStatus = createAlias(index, alias);
						} else {
							final String indexErrMgs = String.format("Create index fail, index:%s", index);
							LOG.error(indexErrMgs);
							if (!ignoreAliasError) {
								throw new RuntimeException(indexErrMgs);
							}
						}
					} catch (Exception e) {
						LOG.error(aliasErrMsg, e);
						if (!ignoreAliasError) {
							throw new RuntimeException(aliasErrMsg, e);
						}
					}
				}
			} else {
				aliasStatus = createAlias(index, alias);
			}
			// print log if add alias success.
			if (aliasStatus && LOG.isInfoEnabled()) {
				LOG.info(String.format("Add alias(%s) for index(%s) success.", alias, index));
			}
			// update cache after alias success.
			updateCache(index);
		} catch (Exception e) {
			LOG.error(aliasErrMsg, e);
			if (!ignoreAliasError) {
				throw new RuntimeException(aliasErrMsg, e);
			}
		}
	}

	protected abstract boolean existsInCache(String index);

	protected abstract void updateCache(String index);

	protected abstract boolean indexExists(String index) throws Exception;

	protected abstract boolean createIndex(String index) throws Exception;

	protected abstract boolean aliasExists(String index, String alias) throws Exception;

	protected abstract boolean createAlias(String index, String alias) throws Exception;
}
