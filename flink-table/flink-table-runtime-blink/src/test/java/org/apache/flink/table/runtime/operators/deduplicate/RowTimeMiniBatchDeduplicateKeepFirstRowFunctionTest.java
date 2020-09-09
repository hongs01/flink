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

import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.bundle.KeyedMapBundleOperator;
import org.apache.flink.table.runtime.operators.bundle.trigger.CountBundleTrigger;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.apache.flink.table.runtime.util.StreamRecordUtils.insertRecord;

/**
 * Tests for {@link RowTimeMiniBatchDeduplicateKeepFirstRowFunction}.
 */
@RunWith(Parameterized.class)
public class RowTimeMiniBatchDeduplicateKeepFirstRowFunctionTest extends RowTimeDeduplicateFunctionTestBase {

	private final long miniBatchSize =  4L;
	private final boolean generateUpdateBefore;
	private final boolean generateInsert;

	public RowTimeMiniBatchDeduplicateKeepFirstRowFunctionTest(boolean generateUpdateBefore, boolean generateInsert) {
		this.generateUpdateBefore = generateUpdateBefore;
		this.generateInsert = generateInsert;
	}

	@Test
	public void testRowTimeDeduplicateKeepFirstRow() throws Exception {
		RowTimeMiniBatchDeduplicateKeepFirstRowFunction func = new RowTimeMiniBatchDeduplicateKeepFirstRowFunction(
			inputRowType,
			serializer,
			rowTimeIndex,
			minTtlTime.toMilliseconds(),
			generateUpdateBefore,
			generateInsert);
		CountBundleTrigger trigger = new CountBundleTrigger<RowData>(miniBatchSize);
		KeyedMapBundleOperator<RowData, RowData, RowData, RowData> operator = new KeyedMapBundleOperator(func, trigger);
		OneInputStreamOperatorTestHarness<RowData, RowData> testHarness = createTestHarness(operator);

		testHarness.open();

		testHarness.processElement(insertRecord("key1", 13, 99L));
		testHarness.processElement(insertRecord("key1", 13, 99L));
		testHarness.processElement(insertRecord("key1", 12, 100L));
		testHarness.processElement(insertRecord("key2", 11, 101L));

		// test 1: keep first row with row time
		testHarness.processWatermark(new Watermark(102));
		assertor.assertOutputEqualsSorted("output wrong.", getExpectOutput(1), testHarness.getOutput());

		// do a snapshot, close and restore again
		OperatorSubtaskState snapshot = testHarness.snapshot(0L, 0);
		testHarness.close();

		testHarness = createTestHarness(operator);
		testHarness.setup();
		testHarness.initializeState(snapshot);
		testHarness.open();

		testHarness.processElement(insertRecord("key1", 12, 300L));
		testHarness.processElement(insertRecord("key2", 11, 301L));
		testHarness.processElement(insertRecord("key3", 5, 299L));

		// test 2:  load snapshot state
		testHarness.processWatermark(new Watermark(302));

		assertor.assertOutputEqualsSorted("output wrong.", getExpectOutput(2), testHarness.getOutput());

		// test 3: expire the state
		testHarness.setStateTtlProcessingTime(minTtlTime.toMilliseconds() + 1);
		testHarness.processElement(insertRecord("key1", 12, 400L));
		testHarness.processElement(insertRecord("key2", 11, 401L));
		testHarness.processWatermark(402);

		// ("key1", 13, 99L) and ("key2", 11, 101L) had retired, thus output ("key1", 12, 200L),("key2", 11, 201L)
		assertor.assertOutputEqualsSorted("output wrong.", getExpectOutput(3), testHarness.getOutput());
		testHarness.close();
	}

	private List<Object> getExpectOutput(int testSeq) {
		String key = generateUpdateBefore + "_" + generateInsert + "_" + testSeq;
		return firstRowExpected.get(key);
	}

	@Parameterized.Parameters(name = "generateUpdateBefore = {0}, generateInsert = {1}")
	public static Collection<Boolean[]> runMode() {
		return Arrays.asList(
			new Boolean[] { false, true },
			new Boolean[] { false, false },
			new Boolean[] { true, false },
			new Boolean[] { true, true });
	}
}
