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

package org.apache.flink.table.filesystem;

import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.java.io.CollectionInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.sources.FilterableTableSource;
import org.apache.flink.table.sources.InputFormatTableSource;
import org.apache.flink.table.sources.LimitableTableSource;
import org.apache.flink.table.sources.PartitionableTableSource;
import org.apache.flink.table.sources.ProjectableTableSource;
import org.apache.flink.table.types.DataType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * File system table source.
 */
public class FileSystemTableSource extends InputFormatTableSource<BaseRow> implements
		PartitionableTableSource,
		ProjectableTableSource<BaseRow>,
		LimitableTableSource<BaseRow>,
		FilterableTableSource<BaseRow> {

	private final TableSchema schema;
	private final Path path;
	private final List<String> partitionKeys;
	private final FileSystemFormatFactory formatFactory;

	private final List<Map<String, String>> readPartitions;
	private final int[] selectFields;
	private final Long limit;
	private final List<Expression> predicates;

	public FileSystemTableSource(
			TableSchema schema,
			Path path,
			List<String> partitionKeys,
			FileSystemFormatFactory formatFactory) {
		this.schema = schema;
		this.path = path;
		this.partitionKeys = partitionKeys;
		this.formatFactory = formatFactory;

		this.selectFields = null;
		this.readPartitions = null;
		this.limit = null;
		this.predicates = null;
	}

	private FileSystemTableSource(
			TableSchema schema,
			Path path,
			List<String> partitionKeys,
			FileSystemFormatFactory formatFactory,
			List<Map<String, String>> readPartitions,
			int[] selectFields,
			Long limit,
			List<Expression> predicates) {
		this.schema = schema;
		this.path = path;
		this.partitionKeys = partitionKeys;
		this.formatFactory = formatFactory;
		this.readPartitions = readPartitions;
		this.selectFields = selectFields;
		this.limit = limit;
		this.predicates = predicates;
	}

	@Override
	public InputFormat<BaseRow, ?> getInputFormat() {
		List<Map<String, String>> partitions = partitionKeys.size() == 0 ?
				null :
				(readPartitions == null ? getPartitions() : readPartitions);
		if (partitions != null && partitions.size() == 0) {
			return new CollectionInputFormat<>(new ArrayList<>(), null);
		} else {
			return formatFactory.createReader(new FileSystemFormatFactory.ReaderContext() {

				@Override
				public TableSchema getSchema() {
					return schema;
				}

				@Override
				public List<String> getPartitionKeys() {
					return partitionKeys;
				}

				@Override
				public Path[] getPaths() {
					if (partitions == null) {
						return new Path[] {path};
					} else {
						return partitions.stream()
								.map(FileSystemTableSource.this::toLinkedPartSpec)
								.map(PartitionPathUtils::generatePartitionPath)
								.map(n -> new Path(path, n))
								.toArray(Path[]::new);
					}
				}

				@Override
				public int[] getProjectFields() {
					return readFields();
				}

				@Override
				public long getLimit() {
					return limit == null ? Long.MAX_VALUE : limit;
				}

				@Override
				public List<Expression> getPredicates() {
					return predicates == null ? Collections.emptyList() : predicates;
				}
			});
		}
	}

	private LinkedHashMap<String, String> toLinkedPartSpec(Map<String, String> part) {
		LinkedHashMap<String, String> partSpec = new LinkedHashMap<>();
		for (String partitionKey : partitionKeys) {
			String value = part.get(partitionKey);
			if (value == null) {
				throw new RuntimeException("Incomplete partition spec: " + part);
			}
			partSpec.put(partitionKey, value);
		}
		return partSpec;
	}

	@Override
	public List<Map<String, String>> getPartitions() {
		try {
			return PartitionPathUtils
					.searchPartSpecAndPaths(path.getFileSystem(), path, partitionKeys.size())
					.stream()
					.map(tuple2 -> tuple2.f0)
					.collect(Collectors.toList());
		} catch (Exception e) {
			throw new TableException("Fetch partitions fail.", e);
		}
	}

	@Override
	public FileSystemTableSource applyPartitionPruning(List<Map<String, String>> remainingPartitions) {
		return new FileSystemTableSource(
				schema, path, partitionKeys, formatFactory, remainingPartitions, selectFields, limit, predicates);
	}

	@Override
	public DataType getProducedDataType() {
		int[] fields = readFields();
		String[] schemaFieldNames = schema.getFieldNames();
		DataType[] schemaTypes = schema.getFieldDataTypes();

		return DataTypes.ROW(Arrays.stream(fields)
				.mapToObj(i -> DataTypes.FIELD(schemaFieldNames[i], schemaTypes[i]))
				.toArray(DataTypes.Field[]::new))
				.bridgedTo(BaseRow.class);
	}

	@Override
	public TableSchema getTableSchema() {
		return schema;
	}

	@Override
	public FileSystemTableSource projectFields(int[] fields) {
		return new FileSystemTableSource(
				schema, path, partitionKeys, formatFactory, readPartitions, fields, limit, predicates);
	}

	@Override
	public FileSystemTableSource applyLimit(long limit) {
		return new FileSystemTableSource(
				schema, path, partitionKeys, formatFactory, readPartitions, selectFields, limit, predicates);
	}

	@Override
	public boolean isLimitPushedDown() {
		return limit != null;
	}

	@Override
	public FileSystemTableSource applyPredicate(List<Expression> predicates) {
		return new FileSystemTableSource(
				schema,
				path,
				partitionKeys,
				formatFactory,
				readPartitions,
				selectFields,
				limit,
				new ArrayList<>(predicates));
	}

	@Override
	public boolean isFilterPushedDown() {
		return this.predicates != null;
	}

	private int[] readFields() {
		return selectFields == null ?
				IntStream.range(0, schema.getFieldCount()).toArray() :
				selectFields;
	}

	@Override
	public String explainSource() {
		return super.explainSource() +
				(readPartitions == null ? "" : ", readPartitions=" + readPartitions) +
				(selectFields == null ? "" : ", selectFields=" + Arrays.toString(selectFields)) +
				(limit == null ? "" : ", limit=" + limit) +
				(predicates == null ? "" : ", predicates=" + predicateString());
	}

	private String predicateString() {
		if (predicates != null) {
			return predicates.stream()
					.map(Expression::asSummaryString)
					.collect(Collectors.joining(","));
		} else {
			return "TRUE";
		}
	}
}
