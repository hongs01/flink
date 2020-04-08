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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.TableException;
import org.apache.flink.types.Row;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;

/**
 * Suite tests for {@link IndexGenerator}.
 */
public class IndexGeneratorTest {
	private String[] fieldNames;
	private TypeInformation<?>[] fieldTypes;
	private List<Row> rows;

	@Before
	public void prepareData() {
		fieldNames = new String[] {"id", "item", "log_ts", "log_date", "order_timestamp", "log_time", "local_datetime", "local_date", "local_time", "note"};
		fieldTypes = new TypeInformation<?>[] {
			Types.INT,
			Types.STRING,
			Types.LONG,
			Types.SQL_DATE,
			Types.SQL_TIMESTAMP,
			Types.SQL_TIME,
			Types.LOCAL_DATE_TIME,
			Types.LOCAL_DATE,
			Types.LOCAL_TIME,
			Types.STRING
		};
		rows = new ArrayList<>();
		rows.add(Row.of(
			1,
			"apple",
			Timestamp.valueOf("2020-03-18 12:12:14").getTime(),
			Date.valueOf("2020-03-18"),
			Timestamp.valueOf("2020-03-18 12:12:14"),
			Time.valueOf("12:12:14"),
			LocalDateTime.of(2020, 3, 18, 12, 12, 14, 1000),
			LocalDate.of(2020, 3, 18),
			LocalTime.of(12, 13, 14, 2000),
			"test1"));
		rows.add(Row.of(
			2,
			"peanut",
			Timestamp.valueOf("2020-03-19 12:22:14").getTime(),
			Date.valueOf("2020-03-19"),
			Timestamp.valueOf("2020-03-19 12:22:21"),
			Time.valueOf("12:22:21"),
			LocalDateTime.of(2020, 3, 19, 12, 22, 14, 1000),
			LocalDate.of(2020, 3, 19),
			LocalTime.of(12, 13, 14, 2000),
			"test2"));
	}

	@Test
	public void testDynamicIndexFromTimestamp() {
		IndexGenerator indexGenerator = IndexGeneratorFactory.createIndexGenerator(
			"{order_timestamp|yyyy_MM_dd_HH-ss}_index",
			fieldNames,
			fieldTypes);
		indexGenerator.open();
		Assert.assertEquals("2020_03_18_12-14_index", indexGenerator.generate(rows.get(0)));
		IndexGenerator indexGenerator1 = IndexGeneratorFactory.createIndexGenerator(
			"{order_timestamp|yyyy_MM_dd_HH_mm}_index",
			fieldNames,
			fieldTypes);
		indexGenerator1.open();
		Assert.assertEquals("2020_03_19_12_22_index", indexGenerator1.generate(rows.get(1)));
	}

	@Test
	public void testDynamicIndexFromLocalDateTime() {
		IndexGenerator indexGenerator = IndexGeneratorFactory.createIndexGenerator(
			"{local_datetime|yyyy_MM_dd_HH-ss}_index",
			fieldNames,
			fieldTypes);
		indexGenerator.open();
		Assert.assertEquals("2020_03_18_12-14_index", indexGenerator.generate(rows.get(0)));
		IndexGenerator indexGenerator1 = IndexGeneratorFactory.createIndexGenerator(
			"{local_datetime|yyyy_MM_dd_HH_mm}_index",
			fieldNames,
			fieldTypes);
		indexGenerator1.open();
		Assert.assertEquals("2020_03_19_12_22_index", indexGenerator1.generate(rows.get(1)));
	}

	@Test
	public void testDynamicIndexFromDate() {
		IndexGenerator indexGenerator = IndexGeneratorFactory.createIndexGenerator(
			"my-index-{log_date|yyyy/MM/dd}",
			fieldNames,
			fieldTypes);
		indexGenerator.open();
		Assert.assertEquals("my-index-2020/03/18", indexGenerator.generate(rows.get(0)));
		Assert.assertEquals("my-index-2020/03/19", indexGenerator.generate(rows.get(1)));
	}

	@Test
	public void testDynamicIndexFromLocalDate() {
		IndexGenerator indexGenerator = IndexGeneratorFactory.createIndexGenerator(
			"my-index-{local_date|yyyy/MM/dd}",
			fieldNames,
			fieldTypes);
		indexGenerator.open();
		Assert.assertEquals("my-index-2020/03/18", indexGenerator.generate(rows.get(0)));
		Assert.assertEquals("my-index-2020/03/19", indexGenerator.generate(rows.get(1)));
	}

	@Test
	public void testDynamicIndexFromTime() {
		IndexGenerator indexGenerator = IndexGeneratorFactory.createIndexGenerator(
			"my-index-{log_time|HH-mm}",
			fieldNames,
			fieldTypes);
		indexGenerator.open();
		Assert.assertEquals("my-index-12-12", indexGenerator.generate(rows.get(0)));
		Assert.assertEquals("my-index-12-22", indexGenerator.generate(rows.get(1)));
	}

	@Test
	public void testDynamicIndexFromLocalTime() {
		IndexGenerator indexGenerator = IndexGeneratorFactory.createIndexGenerator(
			"my-index-{local_time|HH-mm}",
			fieldNames,
			fieldTypes);
		indexGenerator.open();
		Assert.assertEquals("my-index-12-13", indexGenerator.generate(rows.get(0)));
		Assert.assertEquals("my-index-12-13", indexGenerator.generate(rows.get(1)));
	}

	@Test
	public void testGeneralDynamicIndex() {
		IndexGenerator indexGenerator = IndexGeneratorFactory.createIndexGenerator(
			"index_{item}",
			fieldNames,
			fieldTypes);
		indexGenerator.open();
		Assert.assertEquals("index_apple", indexGenerator.generate(rows.get(0)));
		Assert.assertEquals("index_peanut", indexGenerator.generate(rows.get(1)));
	}

	@Test
	public void testStaticIndex() {
		IndexGenerator indexGenerator = IndexGeneratorFactory.createIndexGenerator(
			"my-index",
			fieldNames,
			fieldTypes);
		indexGenerator.open();
		Assert.assertEquals("my-index", indexGenerator.generate(rows.get(0)));
		Assert.assertEquals("my-index", indexGenerator.generate(rows.get(1)));
	}

	@Test
	public void testUnknownField() {
		String expectedExceptionMsg = "Unknown column 'unknown_ts' in index pattern 'my-index-{unknown_ts|yyyy-MM-dd}', please check the column name.";
		try {
			IndexGenerator indexGenerator = IndexGeneratorFactory.createIndexGenerator(
				"my-index-{unknown_ts|yyyy-MM-dd}",
				fieldNames,
				fieldTypes);
			indexGenerator.open();
			Assert.assertEquals("my-index", indexGenerator.generate(rows.get(0)));
		} catch (Exception e) {
			Assert.assertEquals(e.getMessage(), expectedExceptionMsg);
		}
	}

	@Test
	public void testUnSupportedType() {
		String expectedExceptionMsg = "Unsupported DataType 'Integer' found in Elasticsearch dynamic index column:, extract time-related pattern only support DataType 'TIMESTAMP', 'DATE' and 'TIME'.";
		try {
			IndexGenerator indexGenerator = IndexGeneratorFactory.createIndexGenerator(
				"my-index-{id|yyyy-MM-dd}",
				fieldNames,
				fieldTypes);
			indexGenerator.open();
			Assert.assertEquals("my-index", indexGenerator.generate(rows.get(0)));
		} catch (TableException e) {
			Assert.assertEquals(expectedExceptionMsg, e.getMessage());
		}
	}

	@Test
	public void testDynamicIndexUnsupportedFormat() {
		String expectedExceptionMsg = "Unsupported field: HourOfDay";
		try {
			IndexGenerator indexGenerator1 = IndexGeneratorFactory.createIndexGenerator(
				"my-index-{local_date|yyyy/MM/dd HH:mm}",
				fieldNames,
				fieldTypes);
			Assert.assertEquals("my-index-2020/03/18", indexGenerator1.generate(rows.get(0)));
			Assert.assertEquals("my-index-2020/03/19", indexGenerator1.generate(rows.get(1)));
		} catch (Exception e) {
			Assert.assertEquals(expectedExceptionMsg, e.getMessage());
		}
	}
}
