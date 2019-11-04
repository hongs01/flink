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

package org.apache.flink.table.tpcds;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.transformations.ShuffleMode;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.runtime.types.TypeInfoDataTypeConverter;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.table.tpcds.schema.Schema;
import org.apache.flink.table.tpcds.schema.TpcdsSchemaProvider;
import org.apache.flink.table.types.utils.TypeConversions;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * End-to-end test for TPC-DS queries.
 */
public class TpcdsTestProgram {

	private static List<String> tpcdsTables = Arrays.asList(
		"catalog_sales", "catalog_returns", "inventory", "store_sales",
		"store_returns", "web_sales", "web_returns", "call_center", "catalog_page",
		"customer", "customer_address", "customer_demographics", "date_dim",
		"household_demographics", "income_band", "item", "promotion", "reason",
		"ship_mode", "store", "time_dim", "warehouse", "web_page", "web_site");
	private static int maxQueryId = 99;
	private static Set variantQuerySet = new HashSet<Integer>(Arrays.asList(14, 23, 24, 39));

	/**
	 * From now, query 85 will run fail when enable TABLE_OPTIMIZER_JOIN_REORDER_ENABLED and
	 * run success when disable TABLE_OPTIMIZER_JOIN_REORDER_ENABLED. It's confirmed a bad
	 * case for join reorder feature without table statistics.
	 * More detail can refer: https://issues.apache.org/jira/browse/FLINK-14455.
	 */
	private static int badCaseIndex = 85;

	public static void main(String[] args) throws Exception {
		ParameterTool params = ParameterTool.fromArgs(args);
		String sourceTablePath = params.getRequired("sourceTablePath");
		String queryPath = params.getRequired("queryPath");
		String sinkTablePath = params.getRequired("sinkTablePath");

		//Execute TPC-DS queries
		for (int i = 39; i < maxQueryId && i != badCaseIndex; i++) {
			// execute custom queries
			TableEnvironment tableEnvironment = prepareTableEnv(sourceTablePath);
			String queryName = "q" + i + ".sql";
			String queryFilePath = queryPath + "/" + queryName;
			String queryString = loadFile2String(queryFilePath);
			Table resultTable = tableEnvironment.sqlQuery(queryString);
			//register sink table
			String sinkTableName = "q" + i + "_sinkTable";
			TableSink sink = new CsvTableSink(
				sinkTablePath + "/" + "q" + i + ".result",
				"|",
				1,
				FileSystem.WriteMode.OVERWRITE);

			tableEnvironment.registerTableSink(sinkTableName,
				new CsvTableSink(
					sinkTablePath + "/" + "q" + i + ".result",
					"|",
					1,
					FileSystem.WriteMode.OVERWRITE)
					.configure(
						resultTable.getSchema().getFieldNames(),
						Arrays.stream(resultTable.getSchema().getFieldDataTypes())
							.map(r -> TypeInfoDataTypeConverter.fromDataTypeToTypeInfo(r))
							.collect(Collectors.toList())
							.toArray(new TypeInformation[0])
					));
			tableEnvironment.insertInto(resultTable, sinkTableName);
			System.out.println(tableEnvironment.explain(resultTable));
			tableEnvironment.execute(queryName);

			// execute variant queries
			if (variantQuerySet.contains(i)) {
				tableEnvironment = prepareTableEnv(sourceTablePath);
				queryName = "q" + i + "a.sql";
				queryFilePath = queryPath + "/" + queryName;
				queryString = loadFile2String(queryFilePath);
				resultTable = tableEnvironment.sqlQuery(queryString);
				//register sink table
				sinkTableName = "q" + i + "a_sinkTable";
				tableEnvironment.registerTableSink(sinkTableName,
					new CsvTableSink(
						sinkTablePath + "/" + "q" + i + "a.result",
						"|",
						1,
						FileSystem.WriteMode.OVERWRITE)
						.configure(
							resultTable.getSchema().getFieldNames(),
							Arrays.stream(resultTable.getSchema().getFieldDataTypes())
								.map(r -> TypeInfoDataTypeConverter.fromDataTypeToTypeInfo(r))
								.collect(Collectors.toList())
								.toArray(new TypeInformation[0])
						));
				tableEnvironment.insertInto(resultTable, sinkTableName);
				tableEnvironment.execute(queryName);
			}
		}

		//execute query85 which need to disable TABLE_OPTIMIZER_JOIN_REORDER_ENABLED
		TableEnvironment tableEnvironment = prepareTableEnv(sourceTablePath);
		tableEnvironment.getConfig().getConfiguration()
			.setBoolean(OptimizerConfigOptions.TABLE_OPTIMIZER_JOIN_REORDER_ENABLED, false);

		String queryName = "q" + badCaseIndex + ".sql";
		String queryFilePath = queryPath + "/" + queryName;
		String queryString = loadFile2String(queryFilePath);
		Table resultTable = tableEnvironment.sqlQuery(queryString);
		//register sink table
		String sinkTableName = "q" + badCaseIndex + "_sinkTable";
		tableEnvironment.registerTableSink("sinkTable",
			new CsvTableSink(
				sinkTablePath + "/" + "q" + badCaseIndex + ".result",
				"|",
				1,
				FileSystem.WriteMode.OVERWRITE)
				.configure(
					resultTable.getSchema().getFieldNames(),
					Arrays.stream(resultTable.getSchema().getFieldDataTypes())
						.map(r -> TypeInfoDataTypeConverter.fromDataTypeToTypeInfo(r))
						.collect(Collectors.toList())
						.toArray(new TypeInformation[0])
				));
		tableEnvironment.insertInto(resultTable, sinkTableName);
		tableEnvironment.execute(queryName);
	}

	/**
	 * Prepare TableEnvironment for every query.
	 * @param sourceTablePath
	 * @return
	 */
	private static TableEnvironment prepareTableEnv(String sourceTablePath) {
		//Init Table Env
		EnvironmentSettings environmentSettings = EnvironmentSettings
			.newInstance()
			.useBlinkPlanner()
			.inBatchMode()
			.build();
		TableEnvironment tableEnvironment = TableEnvironment.create(environmentSettings);

		//Config Optimizer parameters
		tableEnvironment.getConfig().getConfiguration()
			.setBoolean(OptimizerConfigOptions.TABLE_OPTIMIZER_REUSE_SOURCE_ENABLED, true);
		tableEnvironment.getConfig().getConfiguration()
			.setBoolean(OptimizerConfigOptions.TABLE_OPTIMIZER_REUSE_SUB_PLAN_ENABLED, true);
		tableEnvironment.getConfig().getConfiguration()
			.setBoolean(OptimizerConfigOptions.TABLE_OPTIMIZER_JOIN_REORDER_ENABLED, true);
		tableEnvironment.getConfig().getConfiguration()
			.setBoolean(OptimizerConfigOptions.TABLE_OPTIMIZER_ELIMINATE_CROSS_JOIN_ENABLED, true);
		tableEnvironment.getConfig().getConfiguration()
			.setString(ExecutionConfigOptions.TABLE_EXEC_SHUFFLE_MODE, ShuffleMode.BATCH.toString());

		//Register TPC-DS tables
		tpcdsTables.forEach(table -> {
			Schema schema = TpcdsSchemaProvider.getTpcdsSchema(table);
			CsvTableSource.Builder builder = CsvTableSource.builder();
			builder.path(sourceTablePath + "/" + table + ".csv");
			for (int i = 0; i < schema.getFieldNames().size(); i++) {
				builder.field(
					schema.getFieldNames().get(i),
					TypeConversions.fromDataTypeToLegacyInfo(schema.getFieldTypes().get(i)));
			}
			builder.fieldDelimiter("|");
			builder.lineDelimiter("\n");
			CsvTableSource tableSource = builder.build();
			tableEnvironment.registerTableSource(table, tableSource);
		});

		return tableEnvironment;
	}

	private static String loadFile2String(String filePath) {
		StringBuilder stringBuilder = new StringBuilder();
		try {
			Stream<String> stream = Files.lines(Paths.get(filePath), StandardCharsets.UTF_8);
			stream.forEach(s -> stringBuilder.append(s).append("\n"));
		} catch (IOException e) {
			e.printStackTrace();
		}
		return stringBuilder.toString();
	}

}
