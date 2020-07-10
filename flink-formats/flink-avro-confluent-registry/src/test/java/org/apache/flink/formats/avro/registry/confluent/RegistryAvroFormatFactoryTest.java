/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.formats.avro.registry.confluent;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.TestDynamicTableFactory;
import org.apache.flink.table.runtime.connector.source.ScanRuntimeProviderContext;
import org.apache.flink.table.runtime.typeutils.RowDataTypeInfo;
import org.apache.flink.table.types.logical.RowType;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import static org.apache.flink.core.testutils.FlinkMatchers.containsCause;
import static org.junit.Assert.assertEquals;

/**
 * Tests for the {@link RegistryAvroFormatFactory}.
 */
public class RegistryAvroFormatFactoryTest {
	private TableSchema schema;
	private RowType rowType;
	private String avroSchema;
	private String subject;
	private String registryURL;

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	@Before
	public void before() {
		this.schema = TableSchema.builder()
				.field("a", DataTypes.STRING())
				.field("b", DataTypes.INT())
				.field("c", DataTypes.BOOLEAN())
				.build();
		this.rowType = (RowType) schema.toRowDataType().getLogicalType();
		this.avroSchema = "" +
				"{\"namespace\": \"org.apache.flink.formats.avro.generated\",\n" +
				" \"type\": \"record\",\n" +
				" \"name\": \"Address\",\n" +
				" \"fields\": [\n" +
				"     {\"name\": \"num\", \"type\": \"int\"},\n" +
				"     {\"name\": \"street\", \"type\": \"string\"},\n" +
				"     {\"name\": \"city\", \"type\": \"string\"},\n" +
				"     {\"name\": \"state\", \"type\": \"string\"},\n" +
				"     {\"name\": \"zip\", \"type\": \"string\"}\n" +
				"  ]\n" +
				"}";
		this.subject = "test-subject";
		this.registryURL = "http://localhost:8081";
	}

	@Test
	public void testSeDeSchema() {
		final ConfluentRegistryAvroRowDataDeserializationSchema expectedDeser =
				ConfluentRegistryAvroRowDataDeserializationSchema.create(
						avroSchema,
						registryURL,
						rowType,
						new RowDataTypeInfo(rowType));

		final Map<String, String> options = getAllOptions();

		final DynamicTableSource actualSource = createTableSource(options);
		assert actualSource instanceof TestDynamicTableFactory.DynamicTableSourceMock;
		TestDynamicTableFactory.DynamicTableSourceMock scanSourceMock =
				(TestDynamicTableFactory.DynamicTableSourceMock) actualSource;

		DeserializationSchema<RowData> actualDeser = scanSourceMock.valueFormat
				.createRuntimeDecoder(
						ScanRuntimeProviderContext.INSTANCE,
						schema.toRowDataType());

		assertEquals(expectedDeser, actualDeser);

		final ConfluentRegistryAvroRowDataSerializationSchema expectedSer =
				ConfluentRegistryAvroRowDataSerializationSchema.create(
						subject,
						avroSchema,
						registryURL,
						rowType);

		final DynamicTableSink actualSink = createTableSink(options);
		assert actualSink instanceof TestDynamicTableFactory.DynamicTableSinkMock;
		TestDynamicTableFactory.DynamicTableSinkMock sinkMock =
				(TestDynamicTableFactory.DynamicTableSinkMock) actualSink;

		SerializationSchema<RowData> actualSer = sinkMock.valueFormat
				.createRuntimeEncoder(
						null,
						schema.toRowDataType());

		assertEquals(expectedSer, actualSer);
	}

	@Test
	public void testMissingSubjectForSink() {
		thrown.expect(ValidationException.class);
		thrown.expect(
				containsCause(
						new ValidationException("Option avro-sr.schema-registry.subject "
								+ "is required for serialization")));

		final Map<String, String> options =
				getModifiedOptions(opts -> opts.remove("avro-sr.schema-registry.subject"));

		createTableSink(options);
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	/**
	 * Returns the full options modified by the given consumer {@code optionModifier}.
	 *
	 * @param optionModifier Consumer to modify the options
	 */
	private Map<String, String> getModifiedOptions(Consumer<Map<String, String>> optionModifier) {
		Map<String, String> options = getAllOptions();
		optionModifier.accept(options);
		return options;
	}

	private Map<String, String> getAllOptions() {
		final Map<String, String> options = new HashMap<>();
		options.put("connector", TestDynamicTableFactory.IDENTIFIER);
		options.put("target", "MyTarget");
		options.put("buffer-size", "1000");

		options.put("format", RegistryAvroFormatFactory.IDENTIFIER);
		options.put("avro-sr.schema-registry.subject", subject);
		options.put("avro-sr.schema-registry.url", registryURL);
		options.put("avro-sr.schema-string", avroSchema);
		return options;
	}

	private DynamicTableSource createTableSource(Map<String, String> options) {
		return FactoryUtil.createTableSource(
				null,
				ObjectIdentifier.of("default", "default", "t1"),
				new CatalogTableImpl(schema, options, "mock source"),
				new Configuration(),
				RegistryAvroFormatFactoryTest.class.getClassLoader());
	}

	private DynamicTableSink createTableSink(Map<String, String> options) {
		return FactoryUtil.createTableSink(
				null,
				ObjectIdentifier.of("default", "default", "t1"),
				new CatalogTableImpl(schema, options, "mock sink"),
				new Configuration(),
				RegistryAvroFormatFactoryTest.class.getClassLoader());
	}
}
