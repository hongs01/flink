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

package org.apache.flink.table.runtime.operators.join.temporal;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.JoinedRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.RowDataUtil;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.generated.JoinCondition;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Optional;

/**
 * The operator for temporal join (FOR SYSTEM_TIME AS OF o.rowtime) on row time, it has no limitation
 * about message types of the left input and right input, this means the operator deals changelog well.
 *
 * <p>For Event-time temporal join, its probe side is a regular table, its build side is a versioned
 * table, the version of versioned table can extract from the build side state. This operator works by
 * keeping on the state collection of probe and build records to process on next watermark. The idea
 * is that between watermarks we are collecting those elements and once we are sure that there will be
 * no updates we emit the correct result and clean up the expired data in state.
 *
 * <p>Cleaning up the state drops all of the "old" values from the probe side, where "old" is defined
 * as older then the current watermark. Build side is also cleaned up in the similar fashion,
 * we sort all "old" values with row time and row kind and then clean up the old values, when clean up
 * the "old" values, if the latest record of all "old" values is retract message which means the version
 * end, we clean all "old" values, if the the latest record is accumulate message which means the version
 * start, we keep the latest one, and clear other "old" values.
 *
 * <p>One more trick is how the emitting results and cleaning up is triggered. It is achieved
 * by registering timers for the keys. We could register a timer for every probe and build
 * side element's event time (when watermark exceeds this timer, that's when we are emitting and/or
 * cleaning up the state). However this would cause huge number of registered timers. For example
 * with following evenTimes of probe records accumulated: {1, 2, 5, 8, 9}, if we
 * had received Watermark(10), it would trigger 5 separate timers for the same key. To avoid that
 * we always keep only one single registered timer for any given key, registered for the minimal
 * value. Upon triggering it, we process all records with event times older then or equal to
 * currentWatermark.
 */
public class TemporalRowTimeJoinOperator
	extends BaseTwoInputStreamOperatorWithStateRetention {

	private static final long serialVersionUID = 6642514795175288193L;

	private static final String NEXT_LEFT_INDEX_STATE_NAME = "next-index";
	private static final String LEFT_STATE_NAME = "left";
	private static final String RIGHT_STATE_NAME = "right";
	private static final String REGISTERED_TIMER_STATE_NAME = "timer";
	private static final String TIMERS_STATE_NAME = "timers";

	private final InternalTypeInfo<RowData> leftType;
	private final InternalTypeInfo<RowData> rightType;
	private final GeneratedJoinCondition generatedJoinCondition;
	private final int leftTimeAttribute;
	private final int rightTimeAttribute;

	/**
	 * The comparator to get the ordered elements of right state.
	 */
	private final ChangelogOrderComparator rightChangelogOrderComparator;

	/**
	 * Incremental index generator for {@link #leftState}'s keys.
	 */
	private transient ValueState<Long> nextLeftIndex;

	/**
	 * Mapping from artificial row index (generated by `nextLeftIndex`) into the left side `Row`.
	 * We can not use List to accumulate Rows, because we need efficient deletes of the oldest rows.
	 *
	 * <p>TODO: this could be OrderedMultiMap[Jlong, Row] indexed by row's timestamp, to avoid
	 * full map traversals (if we have lots of rows on the state that exceed `currentWatermark`).
	 */
	private transient MapState<Long, RowData> leftState;

	/**
	 * Mapping from timestamp to right side `Row`.
	 *
	 * <p>TODO: having `rightState` as an OrderedMapState would allow us to avoid sorting cost
	 * once per watermark
	 */
	private transient MapState<RowData, Long> rightState;

	private transient ValueState<Long> registeredEventTimer;
	private transient TimestampedCollector<RowData> collector;
	private transient InternalTimerService<VoidNamespace> timerService;

	private transient JoinCondition joinCondition;
	private transient JoinedRowData outRow;

	public TemporalRowTimeJoinOperator(
			InternalTypeInfo<RowData> leftType,
			InternalTypeInfo<RowData> rightType,
			GeneratedJoinCondition generatedJoinCondition,
			int leftTimeAttribute,
			int rightTimeAttribute,
			long minRetentionTime,
			long maxRetentionTime) {
		super(minRetentionTime, maxRetentionTime);
		this.leftType = leftType;
		this.rightType = rightType;
		this.generatedJoinCondition = generatedJoinCondition;
		this.leftTimeAttribute = leftTimeAttribute;
		this.rightTimeAttribute = rightTimeAttribute;
		this.rightChangelogOrderComparator = new ChangelogOrderComparator(rightTimeAttribute);
	}

	@Override
	public void open() throws Exception {
		joinCondition = generatedJoinCondition.newInstance(getRuntimeContext().getUserCodeClassLoader());
		joinCondition.setRuntimeContext(getRuntimeContext());
		joinCondition.open(new Configuration());

		nextLeftIndex = getRuntimeContext().getState(
			new ValueStateDescriptor<>(NEXT_LEFT_INDEX_STATE_NAME, Types.LONG));
		leftState = getRuntimeContext().getMapState(
			new MapStateDescriptor<>(LEFT_STATE_NAME, Types.LONG, leftType));
		rightState = getRuntimeContext().getMapState(
			new MapStateDescriptor<>(RIGHT_STATE_NAME, rightType, Types.LONG));
		registeredEventTimer = getRuntimeContext().getState(
			new ValueStateDescriptor<>(REGISTERED_TIMER_STATE_NAME, Types.LONG));

		timerService = getInternalTimerService(
			TIMERS_STATE_NAME, VoidNamespaceSerializer.INSTANCE, this);
		collector = new TimestampedCollector<>(output);
		outRow = new JoinedRowData();
	}

	@Override
	public void processElement1(StreamRecord<RowData> element) throws Exception {
		RowData row = element.getValue();
		leftState.put(getNextLeftIndex(), row);
		registerSmallestTimer(getLeftTime(row)); // Timer to emit and clean up the state

		registerProcessingCleanupTimer();
	}

	@Override
	public void processElement2(StreamRecord<RowData> element) throws Exception {
		RowData row = element.getValue();
		long rowTime = getRightTime(row);
		rightState.put(row, rowTime);
		registerSmallestTimer(rowTime); // Timer to clean up the state

		registerProcessingCleanupTimer();
	}

	@Override
	public void onEventTime(InternalTimer<Object, VoidNamespace> timer) throws Exception {
		registeredEventTimer.clear();
		long lastUnprocessedTime = emitResultAndCleanUpState(timerService.currentWatermark());
		if (lastUnprocessedTime < Long.MAX_VALUE) {
			registerTimer(lastUnprocessedTime);
		}

		// if we have more state at any side, then update the timer, else clean it up.
		if (stateCleaningEnabled) {
			if (lastUnprocessedTime < Long.MAX_VALUE || !rightState.isEmpty()) {
				registerProcessingCleanupTimer();
			} else {
				cleanupLastTimer();
			}
		}
	}

	@Override
	public void close() throws Exception {
		if (joinCondition != null) {
			joinCondition.close();
		}
	}

	/**
	 * @return a row time of the oldest unprocessed probe record or Long.MaxValue, if all records
	 *         have been processed.
	 */
	private long emitResultAndCleanUpState(long currentWatermark) throws Exception {
		List<RowData> rightRowsSorted = getRightRowSorted(rightChangelogOrderComparator);
		long lastUnprocessedTime = Long.MAX_VALUE;

		Iterator<Map.Entry<Long, RowData>> leftIterator = leftState.entries().iterator();
		while (leftIterator.hasNext()) {
			Map.Entry<Long, RowData> entry = leftIterator.next();
			RowData leftRow = entry.getValue();
			long leftTime = getLeftTime(leftRow);

			if (leftTime <= currentWatermark) {
				Optional<RowData> rightRow = latestRightRowToJoin(rightRowsSorted, leftTime);
				if (rightRow.isPresent()) {
					if (joinCondition.apply(leftRow, rightRow.get())) {
						outRow.setRowKind(leftRow.getRowKind());
						outRow.replace(leftRow, rightRow.get());
						collector.collect(outRow);
					}
				}
				leftIterator.remove();
			} else {
				lastUnprocessedTime = Math.min(lastUnprocessedTime, leftTime);
			}
		}

		cleanupExpiredVersionInState(currentWatermark, rightRowsSorted);
		return lastUnprocessedTime;
	}

	/**
	 * Removes all expired version in the versioned table's state according to current watermark.
	 * For example with: rightState = [1(+I), 4(-U), 4(+U), 7(-U), 7(+U), 9(-D), 12(I)],
	 *
	 * <p>If watermark = 6 we can not remove "4(+U)" from rightState because accumulate message means
	 * the start of version, the left elements with row time of 5 or 6 could be joined with (+U,4) later.
	 *
	 * <p>If watermark = 10 we can remove "9(-D)" from rightState because retract message means the
	 * end of version, the left elements with row time bigger than 10 would not join the expired 9.
	 */
	private void cleanupExpiredVersionInState(long currentWatermark, List<RowData> rightRowsSorted) throws Exception {
		int i = 0;
		int indexToKeep = firstIndexToKeep(currentWatermark, rightRowsSorted);

		// the latest data is DELETE message means the correspond version has expired
		if (indexToKeep >= 0 && RowDataUtil.isDeleteMsg(rightRowsSorted.get(indexToKeep))) {
			indexToKeep += 1;
		}
		// clean old version data that behind current watermark
		while (i < indexToKeep) {
			RowData row = rightRowsSorted.get(i);
			rightState.remove(row);
			i += 1;
		}
	}

	/**
	 * The method to be called when a cleanup timer fires.
	 *
	 * @param time The timestamp of the fired timer.
	 */
	@Override
	public void cleanupState(long time) {
		leftState.clear();
		rightState.clear();
	}

	private int firstIndexToKeep(long timerTimestamp, List<RowData> rightRowsSorted) {
		int firstIndexNewerThenTimer =
			indexOfFirstElementNewerThanTimer(timerTimestamp, rightRowsSorted);

		if (firstIndexNewerThenTimer < 0) {
			return rightRowsSorted.size() - 1;
		}
		else {
			return firstIndexNewerThenTimer - 1;
		}
	}

	private int indexOfFirstElementNewerThanTimer(long timerTimestamp, List<RowData> list) {
		ListIterator<RowData> iter = list.listIterator();
		while (iter.hasNext()) {
			if (getRightTime(iter.next()) > timerTimestamp) {
				return iter.previousIndex();
			}
		}
		return -1;
	}

	private Optional<RowData> latestRightRowToJoin(List<RowData> rightRowsSorted, long leftTime) {
		int indexToKeep = firstIndexToKeep(leftTime, rightRowsSorted);
		// the latest data is DELETE means the latest version has expired
		if (indexToKeep < 0 || RowDataUtil.isDeleteMsg(rightRowsSorted.get(indexToKeep))) {
			return Optional.empty();
		} else {
			return Optional.of(rightRowsSorted.get(indexToKeep));
		}
	}

	private void registerSmallestTimer(long timestamp) throws IOException {
		Long currentRegisteredTimer = registeredEventTimer.value();
		if (currentRegisteredTimer == null) {
			registerTimer(timestamp);
		} else if (currentRegisteredTimer > timestamp) {
			timerService.deleteEventTimeTimer(VoidNamespace.INSTANCE, currentRegisteredTimer);
			registerTimer(timestamp);
		}
	}

	private void registerTimer(long timestamp) throws IOException {
		registeredEventTimer.update(timestamp);
		timerService.registerEventTimeTimer(VoidNamespace.INSTANCE, timestamp);
	}

	private List<RowData> getRightRowSorted(ChangelogOrderComparator changelogOrderComparator) throws Exception {
		List<RowData> rightRows = new ArrayList<>();
		for (RowData row : rightState.keys()) {
			rightRows.add(row);
		}
		rightRows.sort(changelogOrderComparator);
		return rightRows;
	}

	private long getNextLeftIndex() throws IOException {
		Long index = nextLeftIndex.value();
		if (index == null) {
			index = 0L;
		}
		nextLeftIndex.update(index + 1);
		return index;
	}

	private long getLeftTime(RowData leftRow) {
		return leftRow.getLong(leftTimeAttribute);
	}

	private long getRightTime(RowData rightRow) {
		return rightRow.getLong(rightTimeAttribute);
	}


	// ------------------------------------------------------------------------------------------

	private static class ChangelogOrderComparator implements Comparator<RowData>, Serializable {

		private static final long serialVersionUID = 8160134014590716914L;

		private final int timeAttribute;

		private ChangelogOrderComparator(int timeAttribute) {
			this.timeAttribute = timeAttribute;
		}

		@Override
		public int compare(RowData o1, RowData o2) {
			// order by row time asc
			long o1Time = o1.getLong(timeAttribute);
			long o2Time = o2.getLong(timeAttribute);
			int timeCom = Long.compare(o1Time, o2Time);
			if (timeCom != 0) {
				return timeCom;
			}
			// order by row kind asc (INSERT < UPDATE_BEFORE < UPDATE_AFTER < DELETE)
			byte o1KindValue = o1.getRowKind().toByteValue();
			byte o2KindValue = o2.getRowKind().toByteValue();
			return Byte.compare(o1KindValue, o2KindValue);
		}
	}
}
