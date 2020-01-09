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

import org.apache.flink.table.filesystem.FileSystemStreamingSink.CommitAggregateValue;
import org.apache.flink.table.filesystem.FileSystemStreamingSink.GlobalCommitFunction;

import org.junit.Assert;
import org.junit.Test;

import java.util.Set;
import java.util.TreeMap;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.powermock.api.mockito.PowerMockito.doNothing;

/**
 * Test for {@link GlobalCommitFunction}.
 */
public class GlobalCommitFunctionTest {

	@Test
	public void testFunction() throws Exception {
		FileSystemCommitter committer = mock(FileSystemCommitter.class);
		doNothing().when(committer).commitUpToCheckpoint(anyLong());
		GlobalCommitFunction function = new GlobalCommitFunction(3, committer);
		TreeMap<Long, Set<Integer>> accumulator = function.createAccumulator();

		function.add(new CommitAggregateValue(0, 0), accumulator);
		function.add(new CommitAggregateValue(0, 0), accumulator);
		function.add(new CommitAggregateValue(0, 1), accumulator);
		function.add(new CommitAggregateValue(1, 0), accumulator);
		function.add(new CommitAggregateValue(1, 1), accumulator);
		function.add(new CommitAggregateValue(2, 0), accumulator);
		function.add(new CommitAggregateValue(2, 1), accumulator);
		Assert.assertFalse(function.getResult(accumulator));

		function.add(new CommitAggregateValue(1, 2), accumulator);
		Assert.assertTrue(function.getResult(accumulator));
		function.getResult(accumulator);

		Assert.assertEquals("{2=[0, 1]}", accumulator.toString());

		TreeMap<Long, Set<Integer>> acc2 = function.createAccumulator();
		function.add(new CommitAggregateValue(2, 0), acc2);
		function.add(new CommitAggregateValue(2, 2), acc2);
		function.add(new CommitAggregateValue(3, 0), acc2);
		Assert.assertEquals("{2=[0, 1, 2], 3=[0]}", function.merge(accumulator, acc2).toString());
	}
}
