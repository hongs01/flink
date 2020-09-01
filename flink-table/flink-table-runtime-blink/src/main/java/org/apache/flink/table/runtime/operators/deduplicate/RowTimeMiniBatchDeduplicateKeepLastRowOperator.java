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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.runtime.util.StateTtlConfigUtil.createTtlConfig;

/**
 * This operator is used to get the last row with row time for every key partition in miniBatch mode.
 */
public class RowTimeMiniBatchDeduplicateKeepLastRowOperator extends AbstractStreamOperator<RowData> implements
	OneInputStreamOperator<RowData, RowData>, Triggerable<RowData, VoidNamespace> {

	private static final long serialVersionUID = -7994602893547654994L;

	private final InternalTypeInfo<RowData> rowTypeInfo;
	private final TypeSerializer<RowData> valueSerializer;
	private final boolean generateUpdateBefore;
	private final boolean generateInsert;
	private final int rowTimeIndex;
	private final long minRetentionTime;
	private final long miniBatchSize;
	private final LastRowComparator lastRowComparator;

	// state that stores the mapping of key to all previous rows under the key.
	private ListState<RowData> bufferedStateBetweenWatermark;

	private transient TimerService timerService;
	private transient TimestampedCollector<RowData> collector;
	private transient long numOfElements;
	// buffer to keep all key to the latest previous last row mapping
	private transient Map<RowData, RowData> keyToPrevRow;
	private transient Map<RowData, List<RowData>> keyToPrevRowsBuffer;

	public RowTimeMiniBatchDeduplicateKeepLastRowOperator(
			InternalTypeInfo<RowData> rowTypeInfo,
			TypeSerializer<RowData> valueSerializer,
			int rowTimeIndex,
			boolean generateUpdateBefore,
			boolean generateInsert,
			long miniBatchSize,
			long minRetentionTime) {
		this.rowTypeInfo = rowTypeInfo;
		this.valueSerializer = valueSerializer;
		this.rowTimeIndex = rowTimeIndex;
		this.generateUpdateBefore = generateUpdateBefore;
		this.generateInsert = generateInsert;
		this.miniBatchSize = miniBatchSize;
		this.minRetentionTime = minRetentionTime;
		this.lastRowComparator = new LastRowComparator(rowTimeIndex);
	}

	@Override
	public void open() throws Exception {
		InternalTimerService<VoidNamespace> internalTimerService = getInternalTimerService("rowtime-deduplicator-timers",
			VoidNamespaceSerializer.INSTANCE,
			this);
		timerService = new SimpleTimerService(internalTimerService);
		collector = new TimestampedCollector<>(output);

		ListStateDescriptor<RowData> bufferStateDesc = new ListStateDescriptor<RowData>("previous-rows", rowTypeInfo);
		StateTtlConfig ttlConfig = createTtlConfig(minRetentionTime);
		if (ttlConfig.isEnabled()) {
			bufferStateDesc.enableTimeToLive(ttlConfig);
		}
		bufferedStateBetweenWatermark = getRuntimeContext().getListState(bufferStateDesc);

		numOfElements = 0L;
		keyToPrevRow = new HashMap<>();
		keyToPrevRowsBuffer = new HashMap<>();
	}

	@Override
	public void processElement(StreamRecord<RowData> element) throws Exception {
		numOfElements++;
		RowData currentRow = element.getValue();
		Preconditions.checkArgument(currentRow.getRowKind() == RowKind.INSERT);
		long currentEventTime = getElementTime(element.getValue(), rowTimeIndex);

		// ignore late event
		if (currentEventTime <= timerService.currentWatermark()) {
			return;
		}

		RowData key = (RowData) getCurrentKey();
		RowData preRow = keyToPrevRow.get(key);

		if (isLastRow(preRow, currentRow)) {
			RowData copyRow = valueSerializer.copy(currentRow);
			keyToPrevRow.put(key, copyRow);
			bufferMiniBatchRows(key, preRow, copyRow);
			if (numOfElements > miniBatchSize) {
				finishBundle();
			}
		}
	}

	@Override
	public void onEventTime(InternalTimer<RowData, VoidNamespace> timer) throws Exception {
		// send all data when watermark arrived
		Iterable<RowData> bufferedData = bufferedStateBetweenWatermark.get();
		bufferedData.forEach(rowData -> collector.collect(rowData));
		bufferedStateBetweenWatermark.clear();
	}

	@Override
	public void processWatermark(Watermark mark) throws Exception {
		// flush buffer before advanced watermark
		finishBundle();
		super.processWatermark(mark);
	}

	@Override
	public void onProcessingTime(InternalTimer<RowData, VoidNamespace> timer) throws Exception {
	}

	private boolean isLastRow(RowData preRow, RowData currentRow) {
		return preRow == null || getElementTime(preRow, rowTimeIndex) < getElementTime(currentRow, rowTimeIndex);
	}

	private void bufferMiniBatchRows(RowData key, RowData prevRow, RowData currentRow) {
		List<RowData> prevRows = keyToPrevRowsBuffer.get(key);
		if (prevRows == null) {
			prevRows = new ArrayList<>();
		}
		// store all needed data to buffer
		if (generateUpdateBefore || generateInsert) {
			if (prevRow == null) {
				// the first row, send INSERT message
				currentRow.setRowKind(RowKind.INSERT);
				prevRows.add(currentRow);
			} else {
				if (generateUpdateBefore) {
					prevRow.setRowKind(RowKind.UPDATE_BEFORE);
					prevRows.add(prevRow);
				}
				currentRow.setRowKind(RowKind.UPDATE_AFTER);
				prevRows.add(currentRow);
			}
		} else {
			currentRow.setRowKind(RowKind.UPDATE_AFTER);
			prevRows.add(currentRow);
		}
		keyToPrevRowsBuffer.put(key, prevRows);
	}

	private void finishBundle() throws Exception {
		for (Map.Entry<RowData, List<RowData>> entry : keyToPrevRowsBuffer.entrySet()) {
			RowData key = entry.getKey();
			List<RowData> prevRows = entry.getValue();
			prevRows.sort(lastRowComparator);
			setCurrentKey(key);
			RowData prevRow = null;
			for (RowData currentRow: prevRows) {
				bufferedStateBetweenWatermark.add(currentRow);
				// only keep one event timer for per watermark
				if (prevRow != null) {
					timerService.deleteEventTimeTimer(getElementTime(prevRow, rowTimeIndex));
				}
				timerService.registerEventTimeTimer(getElementTime(currentRow, rowTimeIndex));
				prevRow = currentRow;
			}
		}
		keyToPrevRowsBuffer.clear();
	}

	private static long getElementTime(RowData input, int rowTimeIndex) {
		return input.getLong(rowTimeIndex);
	}

	private static class LastRowComparator implements Comparator<RowData>, Serializable {
		private static final long serialVersionUID = 0L;
		private final int rowTimeIndex;

		public LastRowComparator(int rowTimeIndex) {
			this.rowTimeIndex = rowTimeIndex;
		}

		@Override
		public int compare(RowData o1, RowData o2) {
			long t1 = getElementTime(o1, rowTimeIndex);
			long t2 = getElementTime(o2, rowTimeIndex);
			return Long.compare(t1, t2);
		}
	}
}
