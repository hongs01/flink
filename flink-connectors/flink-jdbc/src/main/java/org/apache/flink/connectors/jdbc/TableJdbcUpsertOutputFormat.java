/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connectors.jdbc;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connectors.jdbc.dialect.JdbcDialect;
import org.apache.flink.connectors.jdbc.dialect.JdbcType;
import org.apache.flink.connectors.jdbc.executor.InsertOrUpdateJdbcExecutor;
import org.apache.flink.connectors.jdbc.executor.JdbcBatchStatementExecutor;
import org.apache.flink.types.Row;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.function.Function;

import static org.apache.flink.connectors.jdbc.JdbcUtils.getPrimaryKey;
import static org.apache.flink.util.Preconditions.checkArgument;

class TableJdbcUpsertOutputFormat extends JdbcBatchingOutputFormat<Tuple2<Boolean, Row>, Row, JdbcBatchStatementExecutor<Row>> {
	private static final Logger LOG = LoggerFactory.getLogger(TableJdbcUpsertOutputFormat.class);

	private JdbcBatchStatementExecutor<Row> deleteExecutor;
	private final JdbcDmlOptions dmlOptions;

	TableJdbcUpsertOutputFormat(JdbcConnectionProvider connectionProvider, JdbcDmlOptions dmlOptions, JdbcExecutionOptions batchOptions) {
		super(connectionProvider, batchOptions, ctx -> createUpsertRowExecutor(dmlOptions, ctx), tuple2 -> tuple2.f1);
		this.dmlOptions = dmlOptions;
	}

	@Override
	public void open(int taskNumber, int numTasks) throws IOException {
		super.open(taskNumber, numTasks);
		deleteExecutor = createDeleteExecutor();
		try {
			deleteExecutor.open(connection);
		} catch (SQLException e) {
			throw new IOException(e);
		}
	}

	private JdbcBatchStatementExecutor<Row> createDeleteExecutor() {
		int[] pkFields = Arrays.stream(dmlOptions.getFieldNames()).mapToInt(Arrays.asList(dmlOptions.getFieldNames())::indexOf).toArray();
		JdbcType[] pkTypes = dmlOptions.getFieldTypes() == null ? null :
				Arrays.stream(pkFields).mapToObj(f -> dmlOptions.getFieldTypes()[f]).toArray(JdbcType[]::new);
		String deleteSql = dmlOptions.getDialect().getDeleteStatement(dmlOptions.getTableName(), dmlOptions.getFieldNames());
		return createKeyedRowExecutor(dmlOptions.getDialect(), pkFields, pkTypes, deleteSql);
	}

	@Override
	void addToBatch(Tuple2<Boolean, Row> original, Row extracted) throws SQLException {
		if (original.f0) {
			super.addToBatch(original, extracted);
		} else {
			deleteExecutor.addToBatch(extracted);
		}
	}

	@Override
	public synchronized void close() {
		try {
			super.close();
		} finally {
			try {
				deleteExecutor.close();
			} catch (SQLException e) {
				LOG.warn("unable to close delete statement runner", e);
			}
		}
	}

	@Override
	void attemptFlush() throws SQLException {
		super.attemptFlush();
		deleteExecutor.executeBatch();
	}

	private static JdbcBatchStatementExecutor<Row> createKeyedRowExecutor(JdbcDialect dialect, int[] pkFields, JdbcType[] pkTypes, String sql) {
		return JdbcBatchStatementExecutor.keyed(
			sql,
			createRowKeyExtractor(pkFields),
			(st, record) -> dialect
				.getOutputConverter(pkTypes)
				.toExternal(createRowKeyExtractor(pkFields).apply(record), st));
	}

	private static JdbcBatchStatementExecutor<Row> createUpsertRowExecutor(JdbcDmlOptions opt, RuntimeContext ctx) {
		checkArgument(opt.getKeyFields().isPresent());

		int[] pkFields = Arrays.stream(opt.getKeyFields().get()).mapToInt(Arrays.asList(opt.getFieldNames())::indexOf).toArray();
		JdbcType[] pkTypes = opt.getFieldTypes() == null ? null : Arrays.stream(pkFields).mapToObj(f -> opt.getFieldTypes()[f])
			.toArray(JdbcType[]::new);

		String upsertStatement = opt.getDialect()
			.getUpsertStatement(opt.getTableName(), opt.getFieldNames(), opt.getKeyFields().get());

		JdbcDialect dialect = opt.getDialect();
		if (upsertStatement == null) {
			return new InsertOrUpdateJdbcExecutor<>(
					opt.getDialect().getRowExistsStatement(opt.getTableName(), opt.getKeyFields().get()),
					opt.getDialect().getInsertIntoStatement(opt.getTableName(), opt.getFieldNames()),
					opt.getDialect().getUpdateStatement(opt.getTableName(), opt.getFieldNames(), opt.getKeyFields().get()),
					createRowJdbcStatementBuilder(dialect, pkTypes),
					createRowJdbcStatementBuilder(dialect, opt.getFieldTypes()),
					createRowJdbcStatementBuilder(dialect, opt.getFieldTypes()),
					createRowKeyExtractor(pkFields),
					ctx.getExecutionConfig().isObjectReuseEnabled() ? Row::copy : Function.identity());
		} else {
			return createSimpleRowExecutor(dialect, upsertStatement, opt.getFieldTypes(), ctx.getExecutionConfig().isObjectReuseEnabled());

		}

	}

	private static Function<Row, Row> createRowKeyExtractor(int[] pkFields) {
		return row -> getPrimaryKey(row, pkFields);
	}

}