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

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
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
 * This function is used to deduplicate on keys and keeps only last row.
 */
public class RowTimeDeduplicateKeepLastRowFunction
		extends KeyedProcessFunction<RowData, RowData, RowData> {

	private static final long serialVersionUID = -291348892087180350L;
	private final InternalTypeInfo<RowData> rowTypeInfo;
	private final boolean generateUpdateBefore;
	private final boolean generateInsert;

	private final int rowtimeIndex;
	private final long minRetentionTime;
	// state stores previous message under the key.
	private ValueState<RowData> prevState;
	private ListState<RowData> bufferedStateBetweenWatermark;

	public RowTimeDeduplicateKeepLastRowFunction(
			long minRetentionTime,
			InternalTypeInfo<RowData> rowTypeInfo,
			int rowtimeIndex,
			boolean generateUpdateBefore,
			boolean generateInsert) {
		this.minRetentionTime = minRetentionTime;
		this.rowTypeInfo = rowTypeInfo;
		this.rowtimeIndex = rowtimeIndex;
		this.generateUpdateBefore = generateUpdateBefore;
		this.generateInsert = generateInsert;
	}

	@Override
	public void open(Configuration configure) throws Exception {
		super.open(configure);
		ValueStateDescriptor<RowData> stateDesc = new ValueStateDescriptor<>("preRowState", rowTypeInfo);
		ListStateDescriptor<RowData> bufferStateDesc = new ListStateDescriptor<RowData>("bufferBetweenWatermark", rowTypeInfo);
		StateTtlConfig ttlConfig = createTtlConfig(minRetentionTime);
		if (ttlConfig.isEnabled()) {
			stateDesc.enableTimeToLive(ttlConfig);
			bufferStateDesc.enableTimeToLive(ttlConfig);
		}
		prevState = getRuntimeContext().getState(stateDesc);
		bufferedStateBetweenWatermark = getRuntimeContext().getListState(bufferStateDesc);
	}

	@Override
	public void processElement(RowData input, Context ctx, Collector<RowData> out) throws Exception {
		Preconditions.checkArgument(input.getRowKind() == RowKind.INSERT);
		long currentEventTime = getElementTime(input);
		// ignore late event
		if (currentEventTime <= ctx.timerService().currentWatermark()) {
			return;
		}

		RowData prevRow = prevState.value();
		if (!isLastRow(prevRow, input)) {
			return;
		}
		processLastRow(prevRow, input, ctx);
	}

	@Override
	public void onTimer(long timestamp, OnTimerContext ctx, Collector<RowData> out) throws Exception {
		// send all data when watermark arrived
		Iterable<RowData> bufferedData = bufferedStateBetweenWatermark.get();
		bufferedData.forEach(rowData -> out.collect(rowData));
		bufferedStateBetweenWatermark.clear();
	}

	private void processLastRow(RowData prevRow, RowData currentRow, Context ctx) throws Exception {
		prevState.update(currentRow);
		// store all needed data to state
		if (generateUpdateBefore || generateInsert) {
			if (prevRow == null) {
				// the first row, send INSERT message
				currentRow.setRowKind(RowKind.INSERT);
				bufferedStateBetweenWatermark.add(currentRow);
			} else {
				if (generateUpdateBefore) {
					prevRow.setRowKind(RowKind.UPDATE_BEFORE);
					bufferedStateBetweenWatermark.add(prevRow);
				}
				currentRow.setRowKind(RowKind.UPDATE_AFTER);
				bufferedStateBetweenWatermark.add(currentRow);
			}
		} else {
			currentRow.setRowKind(RowKind.UPDATE_AFTER);
			bufferedStateBetweenWatermark.add(currentRow);
		}

		// only keep one event timer for per watermark
		if (prevRow != null) {
			ctx.timerService().deleteEventTimeTimer(getElementTime(prevRow));
		}
		ctx.timerService().registerEventTimeTimer(getElementTime(currentRow));
	}

	private long getElementTime(RowData input) {
		return input.getLong(rowtimeIndex);
	}

	private boolean isLastRow(RowData preRow, RowData currentRow) {
		return preRow == null || getElementTime(preRow) <= getElementTime(currentRow);
	}
}
