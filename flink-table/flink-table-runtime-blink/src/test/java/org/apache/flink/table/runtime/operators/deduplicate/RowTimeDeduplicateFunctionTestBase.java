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

package org.apache.flink.table.runtime.operators.deduplicate;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.bundle.KeyedMapBundleOperator;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.util.BinaryRowDataKeySelector;
import org.apache.flink.table.runtime.util.GenericRowRecordSortComparator;
import org.apache.flink.table.runtime.util.RowDataHarnessAssertor;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.RowKind;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.runtime.util.StreamRecordUtils.record;

/**
 * Base class of tests for all kinds of Event-time DeduplicateFunction.
 */
abstract class RowTimeDeduplicateFunctionTestBase {

	protected Time minTtlTime = Time.milliseconds(10);
	protected InternalTypeInfo inputRowType = InternalTypeInfo.ofFields(
		new VarCharType(VarCharType.MAX_LENGTH),
		new IntType(),
		new BigIntType());
	protected TypeSerializer<RowData> serializer = inputRowType.toSerializer();
	protected int rowTimeIndex = 2;
	protected int rowKeyIndex = 0;
	protected BinaryRowDataKeySelector rowKeySelector = new BinaryRowDataKeySelector(
		new int[]{rowKeyIndex},
		inputRowType.toRowFieldTypes());
	protected RowDataHarnessAssertor assertor = new RowDataHarnessAssertor(
		inputRowType.toRowFieldTypes(),
		new GenericRowRecordSortComparator(rowKeyIndex, inputRowType.toRowFieldTypes()[rowKeyIndex]));

	protected static Map<String, List<Object>> firstRowExpected = firstRowExpectedData();
	protected static Map<String, List<Object>> lastRowExpected = lastRowExpectedData();

	protected OneInputStreamOperatorTestHarness<RowData, RowData> createTestHarness(
		KeyedProcessOperator<RowData, RowData, RowData> operator)
		throws Exception {
		return new KeyedOneInputStreamOperatorTestHarness<>(operator, rowKeySelector, rowKeySelector.getProducedType());
	}

	protected OneInputStreamOperatorTestHarness<RowData, RowData> createTestHarness(
		KeyedMapBundleOperator<RowData, RowData, RowData, RowData> operator)
		throws Exception {
		return new KeyedOneInputStreamOperatorTestHarness<>(operator, rowKeySelector, rowKeySelector.getProducedType());
	}

	private static Map<String, List<Object>> firstRowExpectedData() {
		Map<String, List<Object>>  expected = new HashMap<>();
		// generateUpdateBefore: true, generateInsert: true
		expected.put(
			"true_true_1",
			Arrays.asList(
				record(RowKind.INSERT, "key1", 13, 99L),
				record(RowKind.INSERT, "key2", 11, 101L),
				new Watermark(102))
		);
		expected.put(
			"true_true_2",
			Arrays.asList(
				record(RowKind.INSERT, "key3", 5, 299L),
				new Watermark(302))
		);
		expected.put(
			"true_true_3",
			Arrays.asList(
				record(RowKind.INSERT, "key3", 5, 299L),
				new Watermark(302),
				record(RowKind.INSERT, "key1", 12, 400L),
				record(RowKind.INSERT, "key2", 11, 401L),
				new Watermark(402))
		);

		// generateUpdateBefore: true, generateInsert: false
		expected.put(
			"true_false_1",
			Arrays.asList(
				record(RowKind.INSERT, "key1", 13, 99L),
				record(RowKind.INSERT, "key2", 11, 101L),
				new Watermark(102))
		);
		expected.put(
			"true_false_2",
			Arrays.asList(
				record(RowKind.INSERT, "key3", 5, 299L),
				new Watermark(302))
		);
		expected.put(
			"true_false_3",
			Arrays.asList(
				record(RowKind.INSERT, "key3", 5, 299L),
				new Watermark(302),
				record(RowKind.INSERT, "key1", 12, 400L),
				record(RowKind.INSERT, "key2", 11, 401L),
				new Watermark(402))
		);
		// generateUpdateBefore: false, generateInsert: true
		expected.put(
			"false_true_1",
			Arrays.asList(
				record(RowKind.INSERT, "key1", 13, 99L),
				record(RowKind.INSERT, "key2", 11, 101L),
				new Watermark(102))
		);
		expected.put(
			"false_true_2",
			Arrays.asList(
				record(RowKind.INSERT, "key3", 5, 299L),
				new Watermark(302))
		);
		expected.put(
			"false_true_3",
			Arrays.asList(
				record(RowKind.INSERT, "key3", 5, 299L),
				new Watermark(302),
				record(RowKind.INSERT, "key1", 12, 400L),
				record(RowKind.INSERT, "key2", 11, 401L),
				new Watermark(402))
		);
		// generateUpdateBefore: false, generateInsert: false
		expected.put(
			"false_false_1",
			Arrays.asList(
				record(RowKind.UPDATE_AFTER, "key1", 13, 99L),
				record(RowKind.UPDATE_AFTER, "key2", 11, 101L),
				new Watermark(102))
		);
		expected.put(
			"false_false_2",
			Arrays.asList(
				record(RowKind.UPDATE_AFTER, "key3", 5, 299L),
				new Watermark(302))
		);
		expected.put(
			"false_false_3",
			Arrays.asList(
				record(RowKind.UPDATE_AFTER, "key3", 5, 299L),
				new Watermark(302),
				record(RowKind.UPDATE_AFTER, "key1", 12, 400L),
				record(RowKind.UPDATE_AFTER, "key2", 11, 401L),
				new Watermark(402))
		);
		return expected;
	}

	private static Map<String, List<Object>> lastRowExpectedData() {
		Map<String, List<Object>>  expected = new HashMap<>();
		// generateUpdateBefore: true, generateInsert: true
		expected.put(
			"true_true_1",
			Arrays.asList(
				record(RowKind.INSERT, "key1", 13, 99L),
				record(RowKind.UPDATE_BEFORE, "key1", 13, 99L),
				record(RowKind.UPDATE_AFTER, "key1", 12, 100L),
				record(RowKind.INSERT, "key2", 11, 101L),
				new Watermark(102))
		);
		expected.put(
			"true_true_2",
			Arrays.asList(
				record(RowKind.UPDATE_BEFORE, "key1", 12, 100L),
				record(RowKind.UPDATE_AFTER, "key1", 12, 300L),
				record(RowKind.UPDATE_BEFORE, "key2", 11, 101L),
				record(RowKind.UPDATE_AFTER, "key2", 11, 301L),
				record(RowKind.INSERT, "key3", 5, 299L),
				new Watermark(302))
		);
		expected.put(
			"true_true_3",
			Arrays.asList(
				record(RowKind.UPDATE_BEFORE, "key1", 12, 100L),
				record(RowKind.UPDATE_AFTER, "key1", 12, 300L),
				record(RowKind.UPDATE_BEFORE, "key2", 11, 101L),
				record(RowKind.UPDATE_AFTER, "key2", 11, 301L),
				record(RowKind.INSERT, "key3", 5, 299L),
				new Watermark(302),
				record(RowKind.INSERT, "key1", 12, 400L),
				record(RowKind.INSERT, "key2", 11, 401L),
				new Watermark(402))
		);

		// generateUpdateBefore: true, generateInsert: false
		expected.put(
			"true_false_1",
			Arrays.asList(
				record(RowKind.INSERT, "key1", 13, 99L),
				record(RowKind.UPDATE_BEFORE, "key1", 13, 99L),
				record(RowKind.UPDATE_AFTER, "key1", 12, 100L),
				record(RowKind.INSERT, "key2", 11, 101L),
				new Watermark(102))
		);
		expected.put(
			"true_false_2",
			Arrays.asList(
				record(RowKind.UPDATE_BEFORE, "key1", 12, 100L),
				record(RowKind.UPDATE_AFTER, "key1", 12, 300L),
				record(RowKind.UPDATE_BEFORE, "key2", 11, 101L),
				record(RowKind.UPDATE_AFTER, "key2", 11, 301L),
				record(RowKind.INSERT, "key3", 5, 299L),
				new Watermark(302))
		);
		expected.put(
			"true_false_3",
			Arrays.asList(
				record(RowKind.UPDATE_BEFORE, "key1", 12, 100L),
				record(RowKind.UPDATE_AFTER, "key1", 12, 300L),
				record(RowKind.UPDATE_BEFORE, "key2", 11, 101L),
				record(RowKind.UPDATE_AFTER, "key2", 11, 301L),
				record(RowKind.INSERT, "key3", 5, 299L),
				new Watermark(302),
				record(RowKind.INSERT, "key1", 12, 400L),
				record(RowKind.INSERT, "key2", 11, 401L),
				new Watermark(402))
		);
		// generateUpdateBefore: false, generateInsert: true
		expected.put(
			"false_true_1",
			Arrays.asList(
				record(RowKind.INSERT, "key1", 13, 99L),
				record(RowKind.UPDATE_AFTER, "key1", 12, 100L),
				record(RowKind.INSERT, "key2", 11, 101L),
				new Watermark(102))
		);
		expected.put(
			"false_true_2",
			Arrays.asList(
				record(RowKind.UPDATE_AFTER, "key1", 12, 300L),
				record(RowKind.UPDATE_AFTER, "key2", 11, 301L),
				record(RowKind.INSERT, "key3", 5, 299L),
				new Watermark(302))
		);
		expected.put(
			"false_true_3",
			Arrays.asList(
				record(RowKind.UPDATE_AFTER, "key1", 12, 300L),
				record(RowKind.UPDATE_AFTER, "key2", 11, 301L),
				record(RowKind.INSERT, "key3", 5, 299L),
				new Watermark(302),
				record(RowKind.INSERT, "key1", 12, 400L),
				record(RowKind.INSERT, "key2", 11, 401L),
				new Watermark(402))
		);
		// generateUpdateBefore: false, generateInsert: false
		expected.put(
			"false_false_1",
			Arrays.asList(
				record(RowKind.UPDATE_AFTER, "key1", 13, 99L),
				record(RowKind.UPDATE_AFTER, "key1", 12, 100L),
				record(RowKind.UPDATE_AFTER, "key2", 11, 101L),
				new Watermark(102))
		);
		expected.put(
			"false_false_2",
			Arrays.asList(
				record(RowKind.UPDATE_AFTER, "key1", 12, 300L),
				record(RowKind.UPDATE_AFTER, "key2", 11, 301L),
				record(RowKind.UPDATE_AFTER, "key3", 5, 299L),
				new Watermark(302))
		);
		expected.put(
			"false_false_3",
			Arrays.asList(
				record(RowKind.UPDATE_AFTER, "key1", 12, 300L),
				record(RowKind.UPDATE_AFTER, "key2", 11, 301L),
				record(RowKind.UPDATE_AFTER, "key3", 5, 299L),
				new Watermark(302),
				record(RowKind.UPDATE_AFTER, "key1", 12, 400L),
				record(RowKind.UPDATE_AFTER, "key2", 11, 401L),
				new Watermark(402))
		);
		return expected;
	}
}
