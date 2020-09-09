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
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.context.ExecutionContext;
import org.apache.flink.table.runtime.operators.bundle.MapBundleFunction;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.runtime.operators.deduplicate.DeduplicateFunctionHelper.processLastRowOnRowtime;
import static org.apache.flink.table.runtime.util.StateTtlConfigUtil.createTtlConfig;

/**
 * This function is used to get the last row for every key partition in miniBatch mode.
 */
public class RowTimeMiniBatchDeduplicateKeepLastRowFunction
	extends MapBundleFunction<RowData, List<RowData>, RowData, RowData> {

	private static final long serialVersionUID = 1L;

	private final InternalTypeInfo<RowData> rowTypeInfo;
	private final TypeSerializer<RowData> serializer;
	private final int rowtimeIndex;
	private final long minRetentionTime;
	private final boolean generateUpdateBefore;
	private final boolean generateInsert;

	// state stores a boolean flag to indicate whether key appears before.
	private ValueState<RowData> state;

	public RowTimeMiniBatchDeduplicateKeepLastRowFunction(
		InternalTypeInfo<RowData> rowTypeInfo,
		TypeSerializer<RowData> serializer,
		int rowtimeIndex,
		long minRetentionTime,
		boolean generateUpdateBefore,
		boolean generateInsert) {
		this.rowTypeInfo = rowTypeInfo;
		this.serializer = serializer;
		this.rowtimeIndex = rowtimeIndex;
		this.minRetentionTime = minRetentionTime;
		this.generateUpdateBefore = generateUpdateBefore;
		this.generateInsert = generateInsert;
	}

	@Override
	public void open(ExecutionContext ctx) throws Exception {
		super.open(ctx);
		ValueStateDescriptor<RowData> stateDesc = new ValueStateDescriptor<>("first-row-state", rowTypeInfo);
		StateTtlConfig ttlConfig = createTtlConfig(minRetentionTime);
		if (ttlConfig.isEnabled()) {
			stateDesc.enableTimeToLive(ttlConfig);
		}
		state = ctx.getRuntimeContext().getState(stateDesc);
	}

	@Override
	public List<RowData> addInput(@Nullable List<RowData> value, RowData input) throws Exception {
		if (value == null) {
			value = new ArrayList<>();
		}
		value.add(serializer.copy(input));
		return value;
	}

	@Override
	public void finishBundle(Map<RowData, List<RowData>> buffer, Collector<RowData> out) throws Exception {
		for (Map.Entry<RowData, List<RowData>> entry : buffer.entrySet()) {
			RowData currentKey = entry.getKey();
			List<RowData> bufferedRows = entry.getValue();
			ctx.setCurrentKey(currentKey);
			processLastRowOnRowtime(
				state,
				bufferedRows,
				serializer,
				out,
				rowtimeIndex,
				generateUpdateBefore,
				generateInsert);
		}
	}
}
