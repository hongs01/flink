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

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

/**
 * Result comparator for TPC-DS test, according to the TPC-DS standard specification v2.0.0.
 */
public class TpcdsResultComparator {

	public static void main(String[] args) throws IOException {
		ParameterTool params = ParameterTool.fromArgs(args);
		String expectedPath = params.getRequired("expectedPath");
		String actualPath = params.getRequired("actualPath");

		try (
			BufferedReader expectedReader = new BufferedReader(new FileReader(expectedPath));
			BufferedReader actualReader = new BufferedReader(new FileReader(actualPath));
		) {
			int expectedLineNum = 0;
			int actualLineNum = 0;

			String expectedLine, actualLine;
			while (
				(expectedLine = expectedReader.readLine()) != null &&
					(actualLine = actualReader.readLine()) != null
				) {
				String[] expected = expectedLine.split("\\|");
				expectedLineNum++;
				String[] actual = actualLine.split("\\|");
				actualLineNum++;

				if (expected.length != actual.length) {
					System.out.println(
						"Incorrect number of columns on line " + actualLineNum +
							"! Expecting " + expected.length + " columns, but found " + actual.length + " columns.");
					System.exit(1);
				}
				for (int i = 0; i < expected.length; i++) {
					boolean failed;
					failed = !expected[i].trim().equals(actual[i].trim());

					if (failed) {
						System.out.println("Incorrect result on line " + actualLineNum + " column " + (i + 1) +
							"! Expecting " + expected[i] + ", but found " + actual[i] + ".");
						System.exit(1);
					}
				}
			}

			while (expectedReader.readLine() != null) {
				expectedLineNum++;
			}
			while (actualReader.readLine() != null) {
				actualLineNum++;
			}
			if (expectedLineNum != actualLineNum) {
				System.out.println(
					"Incorrect number of lines! Expecting " + expectedLineNum +
						" lines, but found " + actualLineNum + " lines.");
				System.exit(1);
			}
		}
	}

}
