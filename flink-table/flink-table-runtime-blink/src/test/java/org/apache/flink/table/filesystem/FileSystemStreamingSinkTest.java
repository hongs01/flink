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

import org.apache.flink.api.java.io.TextOutputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.types.Row;

import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Test for {@link FileSystemStreamingSink}.
 */
public class FileSystemStreamingSinkTest {

	@ClassRule
	public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

	private File tmpFile;
	private File outputFile;

	@Before
	public void before() throws IOException {
		tmpFile = TEMP_FOLDER.newFolder();
		outputFile = TEMP_FOLDER.newFolder();
	}

	@Test
	public void testClosingWithoutInput() throws Exception {
		try (OneInputStreamOperatorTestHarness<Row, Object> testHarness = createSink(
				false, new LinkedHashMap<>(), new AtomicReference<>())) {
			testHarness.setup();
			testHarness.open();
		}
	}

	@Test
	public void testRecoveryWithPartition() throws Exception {
		OperatorSubtaskState snapshot;

		try (OneInputStreamOperatorTestHarness<Row, Object> testHarness = createSink(true)) {

			testHarness.setup();
			testHarness.open();

			testHarness.processElement(new StreamRecord<>(Row.of("a1", 1, "p1"), 1L));
			testHarness.processElement(new StreamRecord<>(Row.of("a2", 2, "p1"), 1L));
			testHarness.processElement(new StreamRecord<>(Row.of("a3", 3, "p1"), 1L));
			testHarness.processElement(new StreamRecord<>(Row.of("a2", 2, "p2"), 1L));

			testHarness.snapshot(1L, 1L);

			testHarness.processElement(new StreamRecord<>(Row.of("test1", 2, "p1"), 1L));
			testHarness.processElement(new StreamRecord<>(Row.of("test1", 3, "p1"), 1L));

			snapshot = testHarness.snapshot(2L, 1L);

			Assert.assertEquals(2, getFileContentByPath(new File(tmpFile, "cp-1")).size());
			Assert.assertEquals(1, getFileContentByPath(new File(tmpFile, "cp-2")).size());

			testHarness.processElement(new StreamRecord<>(Row.of("test1", 2, "p1"), 1L));
			testHarness.processElement(new StreamRecord<>(Row.of("test1", 3, "p1"), 1L));

			testHarness.notifyOfCompletedCheckpoint(2);
			testHarness.snapshot(3L, 1L);

			Assert.assertEquals(1, getFileContentByPath(new File(tmpFile, "cp-3")).size());
			Assert.assertEquals(3, getFileContentByPath(outputFile).size());
		}

		try (OneInputStreamOperatorTestHarness<Row, Object> testHarness = createSink(true)) {

			testHarness.setup();
			testHarness.initializeState(snapshot);
			testHarness.open();

			// check clean tmp dir
			Assert.assertEquals(0, getFileContentByPath(new File(tmpFile, "cp-1")).size());
			Assert.assertEquals(0, getFileContentByPath(new File(tmpFile, "cp-2")).size());

			testHarness.processElement(new StreamRecord<>(Row.of("a4", 1, "p1"), 1L));
			testHarness.processElement(new StreamRecord<>(Row.of("a5", 2, "p1"), 1L));
			testHarness.processElement(new StreamRecord<>(Row.of("a6", 3, "p1"), 1L));
			testHarness.processElement(new StreamRecord<>(Row.of("a4", 2, "p3"), 1L));

			testHarness.snapshot(3L, 1L);

			Assert.assertEquals(2, getFileContentByPath(new File(tmpFile, "cp-3")).size());
			Assert.assertEquals(3, getFileContentByPath(outputFile).size());

			testHarness.processElement(new StreamRecord<>(Row.of("test7", 2, "p1"), 1L));
			testHarness.processElement(new StreamRecord<>(Row.of("test8", 3, "p1"), 1L));

			testHarness.snapshot(4L, 1L);

			testHarness.processElement(new StreamRecord<>(Row.of("test7", 2, "p1"), 1L));
			testHarness.processElement(new StreamRecord<>(Row.of("test8", 3, "p1"), 1L));

			testHarness.notifyOfCompletedCheckpoint(4);

			Assert.assertEquals(6, getFileContentByPath(outputFile).size());
		}
	}

	@Test
	public void testRecoveryWithoutPatition() throws Exception {
		OperatorSubtaskState snapshot;

		try (OneInputStreamOperatorTestHarness<Row, Object> testHarness = createSink(false)) {

			testHarness.setup();
			testHarness.open();

			testHarness.processElement(new StreamRecord<>(Row.of("a1", 1, "p1"), 1L));
			testHarness.processElement(new StreamRecord<>(Row.of("a2", 2, "p1"), 1L));
			testHarness.processElement(new StreamRecord<>(Row.of("a3", 3, "p1"), 1L));
			testHarness.processElement(new StreamRecord<>(Row.of("a2", 2, "p2"), 1L));

			testHarness.snapshot(1L, 1L);

			testHarness.processElement(new StreamRecord<>(Row.of("test1", 2, "p1"), 1L));
			testHarness.processElement(new StreamRecord<>(Row.of("test1", 3, "p1"), 1L));

			snapshot = testHarness.snapshot(2L, 1L);

			Assert.assertEquals(1, getFileContentByPath(new File(tmpFile, "cp-1")).size());
			Assert.assertEquals(1, getFileContentByPath(new File(tmpFile, "cp-2")).size());

			testHarness.processElement(new StreamRecord<>(Row.of("test1", 2, "p1"), 1L));
			testHarness.processElement(new StreamRecord<>(Row.of("test1", 3, "p1"), 1L));

			testHarness.notifyOfCompletedCheckpoint(2);
			testHarness.snapshot(3L, 1L);

			Assert.assertEquals(1, getFileContentByPath(new File(tmpFile, "cp-3")).size());
			Assert.assertEquals(2, getFileContentByPath(outputFile).size());
		}

		try (OneInputStreamOperatorTestHarness<Row, Object> testHarness = createSink(false)) {

			testHarness.setup();
			testHarness.initializeState(snapshot);
			testHarness.open();

			// check clean tmp dir
			Assert.assertEquals(0, getFileContentByPath(new File(tmpFile, "cp-1")).size());
			Assert.assertEquals(0, getFileContentByPath(new File(tmpFile, "cp-2")).size());

			testHarness.processElement(new StreamRecord<>(Row.of("a4", 1, "p1"), 1L));
			testHarness.processElement(new StreamRecord<>(Row.of("a5", 2, "p1"), 1L));
			testHarness.processElement(new StreamRecord<>(Row.of("a6", 3, "p1"), 1L));
			testHarness.processElement(new StreamRecord<>(Row.of("a4", 2, "p3"), 1L));

			testHarness.snapshot(3L, 1L);

			Assert.assertEquals(1, getFileContentByPath(new File(tmpFile, "cp-3")).size());
			Assert.assertEquals(2, getFileContentByPath(outputFile).size());

			testHarness.processElement(new StreamRecord<>(Row.of("test7", 2, "p1"), 1L));
			testHarness.processElement(new StreamRecord<>(Row.of("test8", 3, "p1"), 1L));

			testHarness.snapshot(4L, 1L);

			testHarness.processElement(new StreamRecord<>(Row.of("test7", 2, "p1"), 1L));
			testHarness.processElement(new StreamRecord<>(Row.of("test8", 3, "p1"), 1L));

			testHarness.notifyOfCompletedCheckpoint(4);

			Assert.assertEquals(4, getFileContentByPath(outputFile).size());
		}
	}

	private OneInputStreamOperatorTestHarness<Row, Object> createSink(
			boolean partition) throws Exception {
		return createSink(partition, new LinkedHashMap<>(), new AtomicReference<>());
	}

	private OneInputStreamOperatorTestHarness<Row, Object> createSink(
			boolean partition,
			LinkedHashMap<String, String> staticPartitions,
			AtomicReference<FileSystemStreamingSink<Row>> sinkRef) throws Exception {
		String[] columnNames = new String[]{"a", "b", "c"};
		String[] partitionColumns = partition ? new String[]{"c"} : new String[0];

		TableMetaStoreFactory msFactory = new FileSystemCommitterTest.TestMetaStoreFactory(
				new Path(outputFile.getPath()));
		FileSystemStreamingSink<Row> sink = new FileSystemStreamingSink.Builder<Row>()
				.setMetaStoreFactory(msFactory)
				.setTempPath(new Path(tmpFile.getPath()))
				.setPartitionColumns(partitionColumns)
				.setPartitionComputer(
						new RowPartitionComputer("default", columnNames, partitionColumns))
				.setFormatFactory(TextOutputFormat::new)
				.setStaticPartitions(staticPartitions)
				.build();

		sinkRef.set(sink);

		return new OneInputStreamOperatorTestHarness<>(
				new StreamSink<>(sink),
				// test parallelism
				1, 1, 0);
	}

	private static Map<File, String> getFileContentByPath(File directory) throws IOException {
		Map<File, String> contents = new HashMap<>(4);

		if (!directory.exists() || !directory.isDirectory()) {
			return contents;
		}

		final Collection<File> filesInBucket = FileUtils.listFiles(directory, null, true);
		for (File file : filesInBucket) {
			contents.put(file, FileUtils.readFileToString(file));
		}
		return contents;
	}
}
