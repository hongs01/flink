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

import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.factories.TableFormatFactory;
import org.apache.flink.table.types.DataType;

import java.util.List;
import java.util.Optional;

/**
 * File system format factory for creating configured instances of reader and writer.
 */
public interface FileSystemFormatFactory extends TableFormatFactory<BaseRow> {

	InputFormat<BaseRow, ?> createReader(ReaderContext context);

	Optional<Encoder<BaseRow>> createEncoder(WriterContext context);

	Optional<BulkWriter.Factory<BaseRow>> createBulkWriterFactory(WriterContext context);

	/**
	 * Context of {@link #createReader}.
	 */
	interface ReaderContext {

		TableSchema getSchema();

		List<String> getPartitionKeys();

		Path[] getPaths();

		int[] getProjectFields();

		long getLimit();

		List<Expression> getPredicates();
	}

	/**
	 * Context of {@link #createEncoder} and {@link #createBulkWriterFactory}.
	 */
	interface WriterContext {

		DataType[] getFieldTypes();
	}
}
