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

package org.apache.flink.table.filesystem;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.taskexecutor.GlobalAggregateManager;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.table.api.TableException;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * File system sink for streaming jobs.
 */
public class FileSystemStreamingSink<T> extends RichSinkFunction<T>
		implements CheckpointedFunction, CheckpointListener {

	private static final long FIRST_CHECKPOINT_ID = 1L;
	private static final ListStateDescriptor<Long> CP_ID_STATE_DESC =
			new ListStateDescriptor<>("checkpoint-id", LongSerializer.INSTANCE);

	private final FileSystemFactory fsFactory;
	private final TableMetaStoreFactory msFactory;
	private final Path tmpPath;
	private final String[] partitionColumns;
	private final LinkedHashMap<String, String> staticPartitions;
	private final PartitionComputer<T> computer;
	private final OutputFormatFactory<T> formatFactory;

	private transient PartitionWriter<T> writer;
	private transient Configuration parameters;
	private transient int taskId;
	private transient ListState<Long> cpIdState;
	private transient GlobalAggregateManager aggregateManager;
	private transient CloseCleanFunction closeFunction;
	private transient GlobalCommitFunction commitFunction;
	private transient PartitionWriterFactory<T> partitionWriterFactory;

	private FileSystemStreamingSink(
			FileSystemFactory fsFactory,
			TableMetaStoreFactory msFactory,
			Path tmpPath,
			String[] partitionColumns,
			LinkedHashMap<String, String> staticPartitions,
			OutputFormatFactory<T> formatFactory,
			PartitionComputer<T> computer) {
		this.fsFactory = fsFactory;
		this.msFactory = msFactory;
		this.tmpPath = tmpPath;
		this.partitionColumns = partitionColumns;
		this.staticPartitions = staticPartitions;
		this.formatFactory = formatFactory;
		this.computer = computer;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		this.parameters = parameters;
		super.open(parameters);
	}

	@Override
	public void initializeState(FunctionInitializationContext context) throws Exception {
		aggregateManager = ((StreamingRuntimeContext) getRuntimeContext()).getGlobalAggregateManager();
		taskId = getRuntimeContext().getIndexOfThisSubtask();
		closeFunction = new CloseCleanFunction(taskId, fsFactory, tmpPath);

		FileSystemCommitter committer = new FileSystemCommitter(
				fsFactory,
				msFactory,
				false,
				tmpPath,
				partitionColumns.length);
		commitFunction = new GlobalCommitFunction(getRuntimeContext().getNumberOfParallelSubtasks(), committer);
		cpIdState = context.getOperatorStateStore().getUnionListState(CP_ID_STATE_DESC);

		partitionWriterFactory = PartitionWriterFactory.get(
				partitionColumns.length - staticPartitions.size() > 0,
				false,
				staticPartitions);
		long cpId = context.isRestored() ? cpIdState.get().iterator().next() + 1 : FIRST_CHECKPOINT_ID;
		createPartitionWriter(cpId);
	}

	private void closePartitionWriter() throws Exception {
		if (writer != null) {
			writer.close();
			writer = null;
		}
	}

	private void createPartitionWriter(long checkpointId) throws Exception {
		PartitionTempFileManager fileManager = new PartitionTempFileManager(
				fsFactory, tmpPath, taskId, checkpointId);
		writer = partitionWriterFactory.create(
				new PartitionWriter.Context<>(parameters, formatFactory),
				fileManager,
				computer);
	}

	@Override
	public void invoke(T value, Context context) throws Exception {
		writer.write(value);
	}

	@Override
	public void snapshotState(FunctionSnapshotContext context) throws Exception {
		long cpId = context.getCheckpointId();
		cpIdState.clear();
		cpIdState.add(cpId);

		closePartitionWriter();
		createPartitionWriter(cpId + 1);
	}

	@Override
	public void notifyCheckpointComplete(long checkpointId) throws Exception {
		aggregateManager.updateGlobalAggregate("commit", new CommitAggregateValue(checkpointId, taskId), commitFunction);
	}

	@Override
	public void close() throws Exception {
		closePartitionWriter();
		aggregateManager.updateGlobalAggregate("close", taskId, closeFunction);
	}

	static class GlobalCommitFunction implements
			AggregateFunction<CommitAggregateValue, TreeMap<Long, Set<Integer>>, Boolean> {

		private final int numberOfTasks;
		private final FileSystemCommitter committer;

		GlobalCommitFunction(int numberOfTasks, FileSystemCommitter committer) {
			this.numberOfTasks = numberOfTasks;
			this.committer = committer;
		}

		@Override
		public TreeMap<Long, Set<Integer>> createAccumulator() {
			return new TreeMap<>();
		}

		@Override
		public TreeMap<Long, Set<Integer>> add(CommitAggregateValue value, TreeMap<Long, Set<Integer>> accumulator) {
			accumulator.compute(value.checkpointId, (cpId, tasks) -> {
				tasks = tasks == null ? new HashSet<>() : tasks;
				tasks.add(value.taskId);
				return tasks;
			});
			return accumulator;
		}

		@Override
		public Boolean getResult(TreeMap<Long, Set<Integer>> accumulator) {
			Long commitCpId = null;
			for (Map.Entry<Long, Set<Integer>> entry : accumulator.descendingMap().entrySet()) {
				if (entry.getValue().size() == numberOfTasks) {
					commitCpId = entry.getKey();
					try {
						committer.commitUpToCheckpoint(commitCpId);
					} catch (Exception e) {
						throw new TableException("Commit failed.", e);
					}
					break;
				}
			}
			if (commitCpId != null) {
				accumulator.headMap(commitCpId, true).clear();
				return true;
			} else {
				return false;
			}
		}

		@Override
		public TreeMap<Long, Set<Integer>> merge(
				TreeMap<Long, Set<Integer>> accumulator, TreeMap<Long, Set<Integer>> b) {
			b.forEach((cpId, bTasks) -> accumulator.compute(cpId, (key, tasks) -> {
				tasks = tasks == null ? new HashSet<>() : tasks;
				tasks.addAll(bTasks);
				return tasks;
			}));
			return accumulator;
		}
	}

	/**
	 * Global Commit aggregate value to track all task complete.
	 */
	public static class CommitAggregateValue implements Serializable {

		private long checkpointId;
		private int taskId;

		public CommitAggregateValue() {}

		CommitAggregateValue(long checkpointId, int taskId) {
			this.checkpointId = checkpointId;
			this.taskId = taskId;
		}
	}

	private static class CloseCleanFunction implements AggregateFunction<Integer, Set<Integer>, Boolean> {

		private final int numberOfTasks;
		private final FileSystemFactory factory;
		private final Path tmpPath;

		private CloseCleanFunction(int numberOfTasks, FileSystemFactory factory, Path tmpPath) {
			this.numberOfTasks = numberOfTasks;
			this.factory = factory;
			this.tmpPath = tmpPath;
		}

		@Override
		public Set<Integer> createAccumulator() {
			return new HashSet<>();
		}

		@Override
		public Set<Integer> add(Integer value, Set<Integer> accumulator) {
			accumulator.add(value);
			return accumulator;
		}

		@Override
		public Boolean getResult(Set<Integer> accumulator) {
			if (accumulator.size() == numberOfTasks) {
				try {
					factory.create(tmpPath.toUri()).delete(tmpPath, true);
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
				accumulator.clear();
				return true;
			} else {
				return false;
			}
		}

		@Override
		public Set<Integer> merge(Set<Integer> a, Set<Integer> b) {
			HashSet<Integer> ret = new HashSet<>();
			ret.addAll(a);
			ret.addAll(b);
			return ret;
		}
	}

	/**
	 * Builder to build {@link FileSystemOutputFormat}.
	 */
	public static class Builder<T> {

		private String[] partitionColumns;
		private OutputFormatFactory<T> formatFactory;
		private TableMetaStoreFactory metaStoreFactory;
		private Path tmpPath;

		private LinkedHashMap<String, String> staticPartitions = new LinkedHashMap<>();
		private FileSystemFactory fileSystemFactory = FileSystem::get;

		private PartitionComputer<T> computer;

		public Builder<T> setPartitionColumns(String[] partitionColumns) {
			this.partitionColumns = partitionColumns;
			return this;
		}

		public Builder<T> setStaticPartitions(LinkedHashMap<String, String> staticPartitions) {
			this.staticPartitions = staticPartitions;
			return this;
		}

		public Builder<T> setFormatFactory(OutputFormatFactory<T> formatFactory) {
			this.formatFactory = formatFactory;
			return this;
		}

		public Builder<T> setFileSystemFactory(FileSystemFactory fileSystemFactory) {
			this.fileSystemFactory = fileSystemFactory;
			return this;
		}

		public Builder<T> setMetaStoreFactory(TableMetaStoreFactory metaStoreFactory) {
			this.metaStoreFactory = metaStoreFactory;
			return this;
		}

		public Builder<T> setTempPath(Path tmpPath) {
			this.tmpPath = tmpPath;
			return this;
		}

		public Builder<T> setPartitionComputer(PartitionComputer<T> computer) {
			this.computer = computer;
			return this;
		}

		public FileSystemStreamingSink<T> build() {
			checkNotNull(partitionColumns, "partitionColumns should not be null");
			checkNotNull(formatFactory, "formatFactory should not be null");
			checkNotNull(metaStoreFactory, "metaStoreFactory should not be null");
			checkNotNull(tmpPath, "tmpPath should not be null");
			checkNotNull(computer, "partitionComputer should not be null");

			return new FileSystemStreamingSink<>(
					fileSystemFactory,
					metaStoreFactory,
					tmpPath,
					partitionColumns,
					staticPartitions,
					formatFactory,
					computer);
		}
	}
}
