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

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import java.util.List;

/**
 * Utility for deduplicate function.
 */
class DeduplicateFunctionHelper {

	/**
	 * Processes element to deduplicate on keys with process time semantic, sends current element as last row,
	 * retracts previous element if needed.
	 *
	 * @param currentRow latest row received by deduplicate function
	 * @param generateUpdateBefore whether need to send UPDATE_BEFORE message for updates
	 * @param state state of function, null if generateUpdateBefore is false
	 * @param out underlying collector
	 */
	static void processLastRowOnProcTime(
			RowData currentRow,
			boolean generateUpdateBefore,
			boolean generateInsert,
			ValueState<RowData> state,
			Collector<RowData> out) throws Exception {

		checkInsertOnly(currentRow);
		if (generateUpdateBefore || generateInsert) {
			// use state to keep the previous row content if we need to generate UPDATE_BEFORE
			// or use to distinguish the first row, if we need to generate INSERT
			RowData preRow = state.value();
			state.update(currentRow);
			if (preRow == null) {
				// the first row, send INSERT message
				currentRow.setRowKind(RowKind.INSERT);
				out.collect(currentRow);
			} else {
				if (generateUpdateBefore) {
					preRow.setRowKind(RowKind.UPDATE_BEFORE);
					out.collect(preRow);
				}
				currentRow.setRowKind(RowKind.UPDATE_AFTER);
				out.collect(currentRow);
			}
		} else {
			// always send UPDATE_AFTER if INSERT is not needed
			currentRow.setRowKind(RowKind.UPDATE_AFTER);
			out.collect(currentRow);
		}
	}

	/**
	 * Processes element to deduplicate on keys with process time semantic, sends current element if it is first row.
	 *
	 * @param currentRow latest row received by deduplicate function
	 * @param state state of function
	 * @param out underlying collector
	 */
	static void processFirstRowOnProcTime(
			RowData currentRow,
			ValueState<Boolean> state,
			Collector<RowData> out) throws Exception {

		checkInsertOnly(currentRow);
		// ignore record if it is not first row
		if (state.value() != null) {
			return;
		}
		state.update(true);
		// emit the first row which is INSERT message
		out.collect(currentRow);
	}

	/**
	 * Processes element to deduplicate on keys with row time semantic, sends current element if it is last row,
	 * retracts previous element if needed.
	 *
	 * @param state state of function
	 * @param currentRow latest row received by deduplicate function
	 * @param serializer serializer to serialize the data
	 * @param out underlying collector
	 * @param rowtimeIndex index of row time field
	 * @param generateUpdateBefore flag to generate UPDATE_BEFORE message or not
	 * @param generateInsert flag to gennerate INSERT message or not
	 */
	static void processLastRowOnRowtime(
			ValueState<RowData> state,
			RowData currentRow,
			TypeSerializer<RowData> serializer,
			Collector<RowData> out,
			int rowtimeIndex,
			boolean generateUpdateBefore,
			boolean generateInsert) throws Exception {

		checkInsertOnly(currentRow);
		RowData prevRow = state.value();
		if (!isLastRow(prevRow, currentRow, rowtimeIndex)) {
			return;
		}
		state.update(currentRow);

		// store all needed data to state
		if (generateUpdateBefore || generateInsert) {
			if (prevRow == null) {
				// the first row, send INSERT message
				currentRow.setRowKind(RowKind.INSERT);
				out.collect(currentRow);
			} else {
				if (generateUpdateBefore) {
					RowData copyRow = serializer.copy(prevRow);
					copyRow.setRowKind(RowKind.UPDATE_BEFORE);
					out.collect(copyRow);
				}
				currentRow.setRowKind(RowKind.UPDATE_AFTER);
				out.collect(currentRow);
			}
		} else {
			currentRow.setRowKind(RowKind.UPDATE_AFTER);
			out.collect(currentRow);
		}
	}

	/**
	 * Processes element to deduplicate on keys with row time semantic, sends current element if it is last row,
	 * retracts previous element if needed.
	 *
	 * @param state state of function
	 * @param bufferedRows latest rows received by deduplicate function
	 * @param serializer serializer to serialize the data
	 * @param out underlying collector
	 * @param rowtimeIndex index of row time field
	 * @param generateUpdateBefore flag to generate UPDATE_BEFORE message or not
	 * @param generateInsert flag to gennerate INSERT message or not
	 */
	static void processLastRowOnRowtime(
		ValueState<RowData> state,
		List<RowData> bufferedRows,
		TypeSerializer<RowData> serializer,
		Collector<RowData> out,
		int rowtimeIndex,
		boolean generateUpdateBefore,
		boolean generateInsert) throws Exception {

		if (bufferedRows == null) {
			return;
		}

		RowData prevRow = state.value();
		for (RowData currentRow: bufferedRows) {
			checkInsertOnly(currentRow);
			if (!isLastRow(prevRow, currentRow, rowtimeIndex)) {
				continue;
			}
			// store all needed data to state
			if (generateUpdateBefore || generateInsert) {
				if (prevRow == null) {
					// the first row, send INSERT message
					currentRow.setRowKind(RowKind.INSERT);
					out.collect(currentRow);
				} else {
					if (generateUpdateBefore) {
						RowData copyRow = serializer.copy(prevRow);
						copyRow.setRowKind(RowKind.UPDATE_BEFORE);
						out.collect(copyRow);
					}
					currentRow.setRowKind(RowKind.UPDATE_AFTER);
					out.collect(currentRow);
				}
			} else {
				currentRow.setRowKind(RowKind.UPDATE_AFTER);
				out.collect(currentRow);
			}
			prevRow = currentRow;
		}
		state.update(prevRow);
	}

	/**
	 * Processes element to deduplicate on keys with row time semantic, sends current element if it is first row,
	 * retracts previous element if needed.
	 *
	 * @param state state of function
	 * @param currentRow latest row received by deduplicate function
	 * @param serializer serializer to serialize the data
	 * @param out underlying collector
	 * @param rowtimeIndex index of row time field
	 * @param generateUpdateBefore flag to generate UPDATE_BEFORE message or not
	 * @param generateInsert flag to gennerate INSERT message or not
	 */
	static void processFirstRowOnRowTime(
			ValueState<RowData> state,
			RowData currentRow,
			TypeSerializer<RowData> serializer,
			Collector<RowData> out,
			int rowtimeIndex,
			boolean generateUpdateBefore,
			boolean generateInsert) throws Exception {

		checkInsertOnly(currentRow);
		RowData preRow = state.value();
		if (!isFirstRow(preRow, currentRow, rowtimeIndex)) {
			return;
		}

		if (generateUpdateBefore || generateInsert) {
			if (preRow == null) {
				// the first row, send INSERT message
				currentRow.setRowKind(RowKind.INSERT);
				out.collect(currentRow);
			} else {
				if (generateUpdateBefore) {
					RowData copyRow = serializer.copy(preRow);
					copyRow.setRowKind(RowKind.UPDATE_BEFORE);
					out.collect(copyRow);
				}
				currentRow.setRowKind(RowKind.UPDATE_AFTER);
				out.collect(currentRow);
			}
		} else {
			currentRow.setRowKind(RowKind.UPDATE_AFTER);
			out.collect(currentRow);
		}

		state.update(currentRow);
	}

	/**
	 * Processes element to deduplicate on keys with row time semantic, sends current element if it is first row,
	 * retracts previous element if needed.
	 *
	 * @param state state of function
	 * @param bufferedRows latest row received by deduplicate function
	 * @param serializer serializer to serialize the data
	 * @param out underlying collector
	 * @param rowtimeIndex index of row time field
	 * @param generateUpdateBefore flag to generate UPDATE_BEFORE message or not
	 * @param generateInsert flag to gennerate INSERT message or not
	 */
	static void processFirstRowOnRowTime(
		ValueState<RowData> state,
		List<RowData> bufferedRows,
		TypeSerializer<RowData> serializer,
		Collector<RowData> out,
		int rowtimeIndex,
		boolean generateUpdateBefore,
		boolean generateInsert) throws Exception {

		if (bufferedRows == null) {
			return;
		}

		RowData preRow = state.value();
		for (RowData currentRow : bufferedRows) {
			checkInsertOnly(currentRow);
			if (!isFirstRow(preRow, currentRow, rowtimeIndex)) {
				continue;
			}

			if (generateUpdateBefore || generateInsert) {
				if (preRow == null) {
					// the first row, send INSERT message
					currentRow.setRowKind(RowKind.INSERT);
					out.collect(currentRow);
				} else {
					if (generateUpdateBefore) {
						RowData copyRow = serializer.copy(preRow);
						copyRow.setRowKind(RowKind.UPDATE_BEFORE);
						out.collect(copyRow);
					}
					currentRow.setRowKind(RowKind.UPDATE_AFTER);
					out.collect(currentRow);
				}
			} else {
				currentRow.setRowKind(RowKind.UPDATE_AFTER);
				out.collect(currentRow);
			}
			preRow = currentRow;
		}
		state.update(preRow);
	}

	/**
	 * Returns current row is first row or not compared to previous row.
	 */
	static boolean isFirstRow(RowData preRow, RowData currentRow, int rowtimeIndex) {
		return preRow == null || getRowtime(currentRow, rowtimeIndex) < getRowtime(preRow, rowtimeIndex);
	}

	/**
	 * Returns current row is last row or not compared to previous row.
	 */
	static boolean isLastRow(RowData preRow, RowData currentRow, int rowtimeIndex) {
		return preRow == null || getRowtime(preRow, rowtimeIndex) <= getRowtime(currentRow, rowtimeIndex);
	}

	private static long getRowtime(RowData input, int rowtimeIndex) {
		return input.getLong(rowtimeIndex);
	}

	/**
	 * check message should be insert only.
	 */
	private static void checkInsertOnly(RowData currentRow) {
		Preconditions.checkArgument(currentRow.getRowKind() == RowKind.INSERT);
	}

	private DeduplicateFunctionHelper() {

	}
}
