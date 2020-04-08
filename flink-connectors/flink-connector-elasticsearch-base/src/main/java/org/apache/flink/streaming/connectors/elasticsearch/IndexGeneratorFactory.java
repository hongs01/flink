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

package org.apache.flink.streaming.connectors.elasticsearch;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.TableException;
import org.apache.flink.types.Row;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Factory of {@link IndexGenerator}.
 *
 * <p>Flink supports both static index and dynamic index.
 *
 * <p>If you want to have a static index, this option value should be a plain string, e.g. 'myusers',
 * all the records will be consistently written into "myusers" index.
 *
 * <p>If you want to have a dynamic index, you can use '{field_name}' to reference a field value in the
 * record to dynamically generate a target index. You can also use '{field_name|date_format_string}' to
 * convert a field value of TIMESTAMP/DATE/TIME type into the format specified by date_format_string. The
 * date_format_string is compatible with {@link java.text.SimpleDateFormat}. For example, if the option
 * value is 'myusers_{log_ts|yyyy-MM-dd}', then a record with log_ts field value 2020-03-27 12:25:55 will
 * be written into "myusers-2020-03-27" index.
 */
@Internal
public class IndexGeneratorFactory {
	private static final Pattern dynamicIndexPattern = Pattern.compile(".*\\{.+\\}.*");
	private static final Pattern dynamicIndexTimeExtractPattern = Pattern.compile(".*\\{.+\\|.*\\}.*");
	private static final String DEFAULT_DATE_FORMAT = "yyyy-MM-dd";

	private IndexGeneratorFactory() {}

	public static IndexGenerator createIndexGenerator(
			String index,
			String[] fieldNames,
			TypeInformation<?>[] fieldTypes) {
		final boolean isDynamicIndex = checkIsDynamicIndex(index);
		if (!isDynamicIndex) {
			return row -> index;
		} else {
			return createRuntimeIndexGenerator(index, fieldNames, fieldTypes);
		}
	}

	/**
	 * Check general dynamic index is enabled or not by index pattern.
	 */
	private static boolean checkIsDynamicIndex(String index) {
		return dynamicIndexPattern.matcher(index).matches();
	}

	/**
	 * Check time extract dynamic index is enabled or not by index pattern.
	 */
	private static boolean checkIsDynamicIndexWithFormat(String index) {
		return dynamicIndexTimeExtractPattern.matcher(index).matches();
	}

	/**
	 * Extract dynamic index pattern string from index pattern string.
	 */
	private static String extractDynamicIndexPatternStr(String index) {
		int start = index.indexOf("{");
		int end = index.lastIndexOf("}");
		return index.substring(start, end + 1);
	}

	/**
	 * Extract index field position in a fieldNames, return the field position.
	 */
	private static int extractIndexFieldPos(String index, String[] fieldNames, boolean isDynamicIndexWithFormat) {
		List<String> fieldList = Arrays.asList(fieldNames);
		String indexFieldName;
		if (isDynamicIndexWithFormat) {
			indexFieldName = index.substring(index.indexOf("{") + 1, index.indexOf("|"));
		} else {
			indexFieldName = index.substring(index.indexOf("{") + 1, index.indexOf("}"));
		}
		if (!fieldList.contains(indexFieldName)) {
			throw new TableException(String.format("Unknown column '%s' in index pattern '%s', please check the column name.",
				indexFieldName, index));
		}
		return fieldList.indexOf(indexFieldName);
	}

	/**
	 * Extract dateTime format by the date format that extracted from index pattern string.
	 */
	private static String extractDateFormat(String index) {
		if (checkIsDynamicIndexWithFormat(index)) {
			return index.substring(index.indexOf("|") + 1, index.indexOf("}"));
		} else {
			return DEFAULT_DATE_FORMAT;
		}
	}

	private static IndexGenerator createRuntimeIndexGenerator(
			String index,
			String[] fieldNames,
			TypeInformation<?>[] fieldTypes) {
		final String dynamicIndexPatternStr = extractDynamicIndexPatternStr(index);
		final String indexPrefix = index.substring(0, index.indexOf(dynamicIndexPatternStr));
		final String indexSuffix = index.substring(indexPrefix.length() + dynamicIndexPatternStr.length());

		final boolean isDynamicIndexWithFormat = checkIsDynamicIndexWithFormat(index);
		final int indexFieldPos = extractIndexFieldPos(index, fieldNames, isDynamicIndexWithFormat);
		// time extract dynamic index pattern
		if (isDynamicIndexWithFormat) {
			final TypeInformation<?> indexFieldType = fieldTypes[indexFieldPos];
			final String dateTimeFormat = extractDateFormat(index);
			// DataTypes.SQL_TIMESTAMP
			if (indexFieldType == Types.LOCAL_DATE_TIME) {
				return new IndexGenerator() {

					private transient DateTimeFormatter dateTimeFormatter;

					@Override
					public void open() {
						this.dateTimeFormatter = DateTimeFormatter.ofPattern(dateTimeFormat);
					}

					@Override
					public String generate(Row row) {
						LocalDateTime indexFiled = (LocalDateTime) row.getField(indexFieldPos);
						String indexFiledValueStr = indexFiled.format(dateTimeFormatter);
						return indexPrefix.concat(indexFiledValueStr).concat(indexSuffix);
					}
				};
			}
			else if (indexFieldType == Types.SQL_TIMESTAMP) {
				return new IndexGenerator() {

					private transient DateTimeFormatter dateTimeFormatter;

					@Override
					public void open() {
						this.dateTimeFormatter = DateTimeFormatter.ofPattern(dateTimeFormat);
					}

					@Override
					public String generate(Row row) {
						Timestamp indexFiled = (Timestamp) row.getField(indexFieldPos);
						String indexFiledValueStr = indexFiled.toLocalDateTime().format(dateTimeFormatter);
						return indexPrefix.concat(indexFiledValueStr).concat(indexSuffix);
					}
				};
			}
			// DataTypes.SQL_DATE
			else if (indexFieldType == Types.LOCAL_DATE) {
				return new IndexGenerator() {

					private transient DateTimeFormatter dateTimeFormatter;

					@Override
					public void open() {
						this.dateTimeFormatter = DateTimeFormatter.ofPattern(dateTimeFormat);
					}

					@Override
					public String generate(Row row) {
						if (dateTimeFormatter == null) {
							open();
						}
						LocalDate indexFiled = (LocalDate) row.getField(indexFieldPos);
						String indexFiledValueStr = indexFiled.format(dateTimeFormatter);
						return indexPrefix.concat(indexFiledValueStr).concat(indexSuffix);
					}
				};
			} else if (indexFieldType == Types.SQL_DATE) {
				return new IndexGenerator() {

					private transient DateTimeFormatter dateTimeFormatter;

					@Override
					public void open() {
						this.dateTimeFormatter = DateTimeFormatter.ofPattern(dateTimeFormat);
					}

					@Override
					public String generate(Row row) {
						if (dateTimeFormatter == null) {
							open();
						}
						Date indexFiled = (Date) row.getField(indexFieldPos);
						String indexFiledValueStr = indexFiled.toLocalDate().format(dateTimeFormatter);
						return indexPrefix.concat(indexFiledValueStr).concat(indexSuffix);
					}
				};
			} // DataTypes.TIME
			else if (indexFieldType == Types.LOCAL_TIME) {
				return new IndexGenerator() {

					private transient DateTimeFormatter dateTimeFormatter;

					@Override
					public void open() {
						this.dateTimeFormatter = DateTimeFormatter.ofPattern(dateTimeFormat);
					}

					@Override
					public String generate(Row row) {
						if (dateTimeFormatter == null) {
							open();
						}
						LocalTime indexFiled = (LocalTime) row.getField(indexFieldPos);
						String indexFiledValueStr = indexFiled.format(dateTimeFormatter);
						return indexPrefix.concat(indexFiledValueStr).concat(indexSuffix);
					}
				};
			} else if (indexFieldType == Types.SQL_TIME) {
				return new IndexGenerator() {

					private transient DateTimeFormatter dateTimeFormatter;

					@Override
					public void open() {
						this.dateTimeFormatter = DateTimeFormatter.ofPattern(dateTimeFormat);
					}

					@Override
					public String generate(Row row) {
						if (dateTimeFormatter == null) {
							open();
						}

						Time indexFiled = (Time) row.getField(indexFieldPos);
						String indexFiledValueStr = indexFiled.toLocalTime().format(dateTimeFormatter);
						return indexPrefix.concat(indexFiledValueStr).concat(indexSuffix);
					}
				};
			} else {
				throw new TableException(String.format("Unsupported DataType '%s' found in Elasticsearch dynamic index column:, " +
					"extract time-related pattern only support DataType 'TIMESTAMP', 'DATE' and 'TIME'.", indexFieldType));
			}
		}
		// general dynamic index pattern
		return row -> {
			Object indexFiled = row.getField(indexFieldPos);
			return indexPrefix.concat(indexFiled == null ? index : indexFiled.toString()).concat(indexSuffix);
		};
	}

}
