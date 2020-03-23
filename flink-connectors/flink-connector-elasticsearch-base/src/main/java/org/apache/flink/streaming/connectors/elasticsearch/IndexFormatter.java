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

import java.io.Serializable;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * User can use {@link IndexFormatter} to format index value from index pattern.
 *
 * <p>If the index is a dynamic index, the index pattern include general pattern like
 * 'connector.index'='my-index-{item}' and time extract pattern like 'connector.index'='my-index-{log_ts|yyyy-MM-dd}'.
 *
 * <p>For general pattern:
 * 'item' is the index field comes from any DataType column.
 *
 * <p>For time extract pattern:
 * 'log_ts' is the index field comes from a BIGINT/SQL_TIMESTAMP/SQL_DATE column.
 * 'yyyy-MM-dd' is the date format follows the {@link DateTimeFormatter} syntax.
 * '{log_ts|yyyy-MM-dd}' is the time extract pattern for index in a dynamic index pattern.
 */
@Internal
public class IndexFormatter implements Serializable {

	private static final Pattern dynamicIndexPattern = Pattern.compile(".*\\{.+\\}.*");
	private static final Pattern dynamicIndexTimeExtractPattern = Pattern.compile(".*\\{.+\\|.*\\}.*");

	private final String index;
	private final String[] fieldNames;
	private final TypeInformation<?>[] fieldTypes;
	private final boolean dynamicIndexEnabled;
	private final boolean dynamicIndexTimeExtractEnabled;
	private final IndexRuntimeConverter indexRuntimeConverter;

	IndexFormatter(String index, String[] fieldNames, TypeInformation<?>[] fieldTypes) {
		this.index = index;
		this.fieldNames = fieldNames;
		this.fieldTypes = fieldTypes;
		this.dynamicIndexEnabled = checkDynamicIndexEnabled();
		this.dynamicIndexTimeExtractEnabled = checkDynamicIndexTimeExtractEnabled();
		this.indexRuntimeConverter = createIndexRuntimeConverter();
	}

	/**
	 * Builder for {@link IndexFormatter}.
	 */
	public static class Builder {
		private String index;
		private String[] fieldNames;
		private TypeInformation<?>[] fieldTypes;

		private Builder() {
			// private constructor
		}

		public Builder index(String index) {
			this.index = index;
			return this;
		}

		public Builder fieldNames(String[] fieldNames) {
			this.fieldNames = fieldNames;
			return this;
		}

		public Builder fieldTypes(TypeInformation<?>[] fieldTypes) {
			this.fieldTypes = fieldTypes;
			return this;
		}

		public IndexFormatter build() {
			return new IndexFormatter(index, fieldNames, fieldTypes);
		}
	}

	/**
	 * Creates a new builder for {@link IndexFormatter}.
	 */
	public static Builder builder() {
		return new Builder();
	}

	public boolean isDynamicIndexEnabled() {
		return dynamicIndexEnabled;
	}

	/**
	 * Return dynamic index if dynamic index is enabled, else return the static index.
	 */
	public String getFormattedIndex(Row row) {
		return indexRuntimeConverter.convert(row);
	}

	/**
	 * Check general dynamic index is enabled or not by index pattern.
	 */
	private boolean checkDynamicIndexEnabled() {
		return dynamicIndexPattern.matcher(index).matches();
	}

	/**
	 * Check time extract dynamic index is enabled or not by index pattern.
	 */
	private boolean checkDynamicIndexTimeExtractEnabled() {
		return dynamicIndexTimeExtractPattern.matcher(index).matches();
	}

	/**
	 * Extract dynamic index pattern string from index pattern string.
	 */
	private String extractDynamicIndexPatternStr() {
		String dynamicIndexPatternStr = null;
		if (dynamicIndexEnabled) {
			int start = index.indexOf("{");
			int end = index.lastIndexOf("}");
			dynamicIndexPatternStr = index.substring(start, end + 1);
		}
		return dynamicIndexPatternStr;
	}

	/**
	 * Extract index field position in a fieldNames, return the field position.
	 */
	private int extractIndexFieldPos(String[] fieldNames) {
		int pos = 0;
		if (dynamicIndexEnabled) {
			List<String> fieldList = Arrays.asList(fieldNames);
			String indexFieldName;
			if (dynamicIndexTimeExtractEnabled) {
				indexFieldName = index.substring(index.indexOf("{") + 1, index.indexOf("|"));
			} else {
				indexFieldName = index.substring(index.indexOf("{") + 1, index.indexOf("}"));
			}
			if (!fieldList.contains(indexFieldName)) {
				throw new TableException(String.format("Unknown column '%s' in index pattern '%s', please check the column name.",
					indexFieldName, index));
			}
			pos = fieldList.indexOf(indexFieldName);
		}
		return pos;
	}

	/**
	 * Extract {@link DateTimeFormatter} by the date format that extracted from index pattern string.
	 */
	private DateTimeFormatter extractDateFormat() {
		String pattern = index.substring(index.indexOf("|") + 1, index.indexOf("}"));
		return DateTimeFormatter.ofPattern(pattern);
	}

	/**
	 * Runtime converter that converts the index field to string.
	 */
	@FunctionalInterface
	private interface IndexRuntimeConverter extends Serializable{
		String convert(Row row);
	}

	private IndexRuntimeConverter createIndexRuntimeConverter() {
		final String dynamicIndexPatternStr = extractDynamicIndexPatternStr();

		if (dynamicIndexEnabled) {
			final int indexFieldPos = extractIndexFieldPos(fieldNames);
			// time extract dynamic index pattern
			if (dynamicIndexTimeExtractEnabled) {
				final TypeInformation<?>indexFieldType = fieldTypes[indexFieldPos];
				// DataTypes.LONG
				if (indexFieldType == Types.LONG) {
					return (row) -> {
						Long indexFiled = (Long) row.getField(indexFieldPos);
						DateTimeFormatter dateFormat = extractDateFormat();
						String indexFiledValueStr = LocalDateTime.ofInstant(
							Instant.ofEpochMilli(indexFiled), ZoneId.systemDefault()).format(dateFormat);
						return index.replace(dynamicIndexPatternStr, indexFiledValueStr);
					};
				}
				// DataTypes.SQL_TIMESTAMP
				else if (indexFieldType == Types.LOCAL_DATE_TIME) {
					return (row) -> {
						String indexFiled = row.getField(indexFieldPos).toString();
						DateTimeFormatter dateFormat = extractDateFormat();
						String indexFiledValueStr = LocalDateTime.parse(indexFiled).format(dateFormat);
						return index.replace(dynamicIndexPatternStr, indexFiledValueStr);
					};
				} else if (indexFieldType == Types.SQL_TIMESTAMP) {
					return (row) -> {
						String indexFiled = row.getField(indexFieldPos).toString();
						DateTimeFormatter dateFormat = extractDateFormat();
						String indexFiledValueStr = Timestamp.valueOf(indexFiled).toLocalDateTime().format(dateFormat);
						return index.replace(dynamicIndexPatternStr, indexFiledValueStr);
					};
				}
				// DataTypes.SQL_DATE
				else if (indexFieldType == Types.LOCAL_DATE) {
					return (row) -> {
						String indexFiled = row.getField(indexFieldPos).toString();
						DateTimeFormatter dateFormat = extractDateFormat();
						String indexFiledValueStr = LocalDate.parse(indexFiled).format(dateFormat);
						return index.replace(dynamicIndexPatternStr, indexFiledValueStr);
					};
				} else if (indexFieldType == Types.SQL_DATE) {
					return (row) -> {
						String indexFiled = row.getField(indexFieldPos).toString();
						DateTimeFormatter dateFormat = extractDateFormat();
						String indexFiledValueStr = Date.valueOf(indexFiled).toLocalDate().format(dateFormat);
						return index.replace(dynamicIndexPatternStr, indexFiledValueStr);
					};
				} else {
					throw new TableException(String.format("Unsupported DataType '%s' found in Elasticsearch dynamic index column:, " +
						"extract time-related pattern only support DataType 'BIGINT'、'SQL_TIMESTAMP' and 'SQL_DATE'.", indexFieldType));
				}
			}

			// general dynamic index pattern
			return (row) -> {
				Object indexFiled = row.getField(indexFieldPos);
				return index.replace(dynamicIndexPatternStr, indexFiled.toString());
			};
		}
		// static index
		return (row) -> index;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (!(o instanceof IndexFormatter)) {
			return false;
		}
		IndexFormatter that = (IndexFormatter) o;
		return index.equals(that.index) &&
			Arrays.equals(fieldNames, that.fieldNames) &&
			Arrays.equals(fieldTypes, that.fieldTypes);
	}

	@Override
	public int hashCode() {
		int result = Objects.hash(index);
		result = 31 * result + Arrays.hashCode(fieldNames);
		result = 31 * result + Arrays.hashCode(fieldTypes);
		return result;
	}
}
