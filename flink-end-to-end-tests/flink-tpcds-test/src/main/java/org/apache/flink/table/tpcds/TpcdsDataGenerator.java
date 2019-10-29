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

package org.apache.flink.table.tpcds;

import org.apache.flink.api.java.utils.ParameterTool;

import com.teradata.tpcds.Session;
import com.teradata.tpcds.Table;
import com.teradata.tpcds.TableGenerator;

import java.io.File;
import java.io.IOException;
import java.util.Optional;

/**
 * TPC-DS test data generator.
 */
public class TpcdsDataGenerator {

	public static void main(String[] args) throws IOException {
		ParameterTool params = ParameterTool.fromArgs(args);
		String scaleString = params.getRequired("scale");
		double scale = Double.valueOf(scaleString);
		String path = params.getRequired("path");

		generateTable(scale, path);
	}

	private static void generateTable(double scale, String path) throws IOException {
		File dir = new File(path + "/table");
		dir.mkdir();

		for (Table table : Table.getBaseTables()) {
			Session session = new Session(
				scale,
				dir.getPath(),
				".csv",
				Optional.of(table),
				"",
				'|',
				false,
				false,
				1,
				true);
			TableGenerator generator = new TableGenerator(session);
			generator.generateTable(table);
		}
	}
}

