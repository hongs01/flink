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

import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import static org.apache.flink.table.runtime.util.StateTtlConfigUtil.createTtlConfig;

/**
 * This function is used to deduplicate on keys and keeps only first row.
 */
public class RowTimeDeduplicateKeepFirstRowFunction
		extends KeyedProcessFunction<RowData, RowData, RowData> {

	private static final long serialVersionUID = 5865777137707602549L;

	private final InternalTypeInfo<RowData> rowTypeInfo;
	private final int rowtimeIndex;
	private final long minRetentionTime;
	// state stores previous message under the key.
	private ValueState<RowData> prevState;

	public RowTimeDeduplicateKeepFirstRowFunction(
			InternalTypeInfo<RowData> rowTypeInfo,
			int rowtimeIndex,
			long minRetentionTime) {
		this.rowTypeInfo = rowTypeInfo;
		this.rowtimeIndex = rowtimeIndex;
		this.minRetentionTime = minRetentionTime;
	}

	@Override
	public void open(Configuration configure) throws Exception {
		super.open(configure);
		ValueStateDescriptor<RowData> stateDesc = new ValueStateDescriptor<>("first-row-state", rowTypeInfo);
		StateTtlConfig ttlConfig = createTtlConfig(minRetentionTime);
		if (ttlConfig.isEnabled()) {
			stateDesc.enableTimeToLive(ttlConfig);
		}
		prevState = getRuntimeContext().getState(stateDesc);
	}

	@Override
	public void processElement(RowData input, Context ctx, Collector<RowData> out) throws Exception {
		// check message should be insert only.
		Preconditions.checkArgument(input.getRowKind() == RowKind.INSERT);
		long currentEventTime = getElementTime(input);
		// ignore late event
		if (currentEventTime <= ctx.timerService().currentWatermark()) {
			return;
		}

		RowData preRow = prevState.value();
		if (!isFirstRow(preRow, input)) {
			return;
		}
		prevState.update(input);
		if (preRow != null) {
			ctx.timerService().deleteEventTimeTimer(getElementTime(preRow));
		}
		ctx.timerService().registerEventTimeTimer(currentEventTime);
	}

	@Override
	public void onTimer(long timestamp, OnTimerContext ctx, Collector<RowData> out) throws Exception {
		RowData firstRow = prevState.value();
		if (firstRow != null) {
			out.collect(firstRow);
		}
	}

	private long getElementTime(RowData input) {
		return input.getLong(rowtimeIndex);
	}

	private boolean isFirstRow(RowData preRow, RowData currentRow) {
		return preRow == null || getElementTime(currentRow) < getElementTime(preRow);
	}
}
