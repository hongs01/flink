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
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.api.SimpleTimerService;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.api.operators.Triggerable;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Preconditions;

import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.table.runtime.util.StateTtlConfigUtil.createTtlConfig;

/**
 * This operator is used to get the first row with row time for every key partition in miniBatch mode.
 */
public class RowTimeMiniBatchDeduplicateKeepFirstRowOperator extends AbstractStreamOperator<RowData> implements
	OneInputStreamOperator<RowData, RowData>, Triggerable<RowData, VoidNamespace> {

	private static final long serialVersionUID = -7994602893547654994L;

	private final InternalTypeInfo<RowData> rowTypeInfo;
	private final TypeSerializer<RowData> valueSerializer;
	private final boolean generateUpdateBefore;
	private final boolean generateInsert;
	private final int rowtimeIndex;
	private final long minRetentionTime;
	private final long miniBatchSize;

	// state that stores the first row under the key.
	private ValueState<RowData> prevState;

	private transient TimerService timerService;
	private transient TimestampedCollector<RowData> collector;
	private transient long numOfElements;
	// buffer to keep all key to the latest previous first row mapping
	private transient Map<RowData, RowData> keyToPrevRow;

	public RowTimeMiniBatchDeduplicateKeepFirstRowOperator(
			InternalTypeInfo<RowData> rowTypeInfo,
			TypeSerializer<RowData> valueSerializer,
			int rowtimeIndex,
			boolean generateUpdateBefore,
			boolean generateInsert,
			long miniBatchSize,
			long minRetentionTime) {
		this.rowTypeInfo = rowTypeInfo;
		this.valueSerializer = valueSerializer;
		this.rowtimeIndex = rowtimeIndex;
		this.generateUpdateBefore = generateUpdateBefore;
		this.generateInsert = generateInsert;
		this.miniBatchSize = miniBatchSize;
		this.minRetentionTime = minRetentionTime;
	}

	@Override
	public void open() throws Exception {
		InternalTimerService<VoidNamespace> internalTimerService = getInternalTimerService("rowtime-deduplicator-timers",
			VoidNamespaceSerializer.INSTANCE,
			this);
		timerService = new SimpleTimerService(internalTimerService);
		collector = new TimestampedCollector<>(output);

		ValueStateDescriptor<RowData> stateDesc = new ValueStateDescriptor<>("first-row-state", rowTypeInfo);
		StateTtlConfig ttlConfig = createTtlConfig(minRetentionTime);
		if (ttlConfig.isEnabled()) {
			stateDesc.enableTimeToLive(ttlConfig);
		}
		prevState = getRuntimeContext().getState(stateDesc);

		numOfElements = 0L;
		keyToPrevRow = new HashMap<>();
	}

	@Override
	public void processElement(StreamRecord<RowData> element) throws Exception {
		numOfElements++;
		RowData currentRow = element.getValue();
		Preconditions.checkArgument(currentRow.getRowKind() == RowKind.INSERT);
		long currentEventTime = getElementTime(element.getValue(), rowtimeIndex);
		// ignore late event
		if (currentEventTime <= timerService.currentWatermark()) {
			return;
		}

		RowData key = (RowData) getCurrentKey();
		RowData preRow = keyToPrevRow.get(key);

		if (isFirstRow(preRow, currentRow)) {
			RowData copyRow = valueSerializer.copy(currentRow);
			keyToPrevRow.put(key, copyRow);
			if (numOfElements > miniBatchSize) {
				finishBundle();
			}
		}
	}

	@Override
	public void onEventTime(InternalTimer<RowData, VoidNamespace> timer) throws Exception {
		RowData firstRow = prevState.value();
		if (firstRow != null) {
			//TODO note
//			System.out.println("duplicate output: " + firstRow.getRowKind() + " "
//				+ firstRow.getString(0) + " "
//				+ firstRow.getLong(1) + " "
//				+ firstRow.getTimestamp(2, 3).toTimestamp() + " ");
			collector.collect(firstRow);
		}
	}

	@Override
	public void onProcessingTime(InternalTimer<RowData, VoidNamespace> timer) throws Exception {
	}

	@Override
	public void processWatermark(Watermark mark) throws Exception {
		// flush buffer before advanced watermark
		finishBundle();
		super.processWatermark(mark);
	}

	private boolean isFirstRow(RowData preRow, RowData currentRow) {
		return preRow == null || getElementTime(currentRow, rowtimeIndex) < getElementTime(preRow, rowtimeIndex);
	}

	private void finishBundle() throws Exception {
		for (Map.Entry<RowData, RowData> entry : keyToPrevRow.entrySet()) {
			RowData key = entry.getKey();
			RowData currentRow = entry.getValue();

			setCurrentKey(key);
			RowData prevRow = prevState.value();
			// only keep one event timer for per watermark
			if (prevRow != null) {
				timerService.deleteEventTimeTimer(getElementTime(prevRow, rowtimeIndex));
			}
			timerService.registerEventTimeTimer(getElementTime(currentRow, rowtimeIndex));

			prevState.update(currentRow);
		}
		keyToPrevRow.clear();
	}

	private static long getElementTime(RowData input, int rowtimeIndex) {
		return input.getLong(rowtimeIndex);
	}

}
