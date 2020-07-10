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

package org.apache.flink.formats.avro;

import org.apache.flink.formats.avro.generated.Address;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.formats.avro.utils.TestDataGenerator;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.TypeConversions;

import org.apache.avro.Schema;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Random;

import static org.apache.flink.formats.avro.utils.AvroTestUtils.writeRecord;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/**
 * Tests for {@link RegistryAvroRowDataDeserializationSchema} and
 * {@link RegistryAvroRowDataSerializationSchema}.
 */
public class RegistryAvroRowDataSeDeSchemaTest {
	private static final String ADDRESS_SCHEMA = "" +
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

	private static final String ADDRESS_SCHEMA_COMPATIBLE = "" +
			"{\"namespace\": \"org.apache.flink.formats.avro.generated\",\n" +
			" \"type\": \"record\",\n" +
			" \"name\": \"Address\",\n" +
			" \"fields\": [\n" +
			"     {\"name\": \"num\", \"type\": \"int\"},\n" +
			"     {\"name\": \"street\", \"type\": \"string\"}\n" +
			"  ]\n" +
			"}";

	private Address address;

	private SchemaCoder.SchemaCoderProvider schemaCoderProvider;

	@Before
	public void before() {
		this.address = TestDataGenerator.generateRandomAddress(new Random());
		this.schemaCoderProvider = () -> new SchemaCoder() {
			@Override
			public Schema readSchema(InputStream in) {
				return Address.getClassSchema();
			}

			@Override
			public void writeSchema(Schema schema, OutputStream out) throws IOException {
				//do nothing
			}
		};
	}

	@Test
	public void testRowDataReadWithFullSchema() throws Exception {
		DataType dataType = TypeConversions.fromLegacyInfoToDataType(AvroSchemaConverter.convertToTypeInfo(ADDRESS_SCHEMA));
		RowType rowType = (RowType) dataType.getLogicalType();
		RegistryAvroRowDataDeserializationSchema deserializer = new RegistryAvroRowDataDeserializationSchema(
				ADDRESS_SCHEMA,
				rowType,
				new RowDataTypeInfo(rowType),
				this.schemaCoderProvider
		);

		deserializer.open(null);

		RowData rowData = deserializer.deserialize(writeRecord(
				address,
				Address.getClassSchema()));
		assertEquals(address.getNum(), Integer.valueOf(rowData.getInt(0)));
		assertEquals(address.getStreet(), rowData.getString(1).toString());
		assertEquals(address.getCity(), rowData.getString(2).toString());
		assertEquals(address.getState(), rowData.getString(3).toString());
		assertEquals(address.getZip(), rowData.getString(4).toString());
	}

	@Test
	public void testRowDataReadWithCompatibleSchema() throws Exception {
		DataType dataType = TypeConversions.fromLegacyInfoToDataType(AvroSchemaConverter.convertToTypeInfo(ADDRESS_SCHEMA));
		RowType rowType = (RowType) dataType.getLogicalType();
		RegistryAvroRowDataDeserializationSchema deserializer = new RegistryAvroRowDataDeserializationSchema(
				ADDRESS_SCHEMA_COMPATIBLE,
				rowType,
				new RowDataTypeInfo(rowType),
				this.schemaCoderProvider
		);

		deserializer.open(null);

		RowData rowData = deserializer.deserialize(writeRecord(
				address,
				Address.getClassSchema()));
		assertThat(rowData.getArity(), is(5));
		assertEquals(address.getNum(), Integer.valueOf(rowData.getInt(0)));
		assertEquals(address.getStreet(), rowData.getString(1).toString());
		assertThat(rowData.isNullAt(2), is(true));
		assertThat(rowData.isNullAt(3), is(true));
		assertThat(rowData.isNullAt(4), is(true));
	}

	@Test
	public void testRowDataWriteReadWithFullSchema() throws Exception {
		DataType dataType = TypeConversions.fromLegacyInfoToDataType(AvroSchemaConverter.convertToTypeInfo(ADDRESS_SCHEMA));
		RowType rowType = (RowType) dataType.getLogicalType();

		RegistryAvroRowDataSerializationSchema serializer = new RegistryAvroRowDataSerializationSchema(
				ADDRESS_SCHEMA,
				this.schemaCoderProvider,
				rowType);

		RegistryAvroRowDataDeserializationSchema deserializer = new RegistryAvroRowDataDeserializationSchema(
				ADDRESS_SCHEMA,
				rowType,
				new RowDataTypeInfo(rowType),
				this.schemaCoderProvider
		);

		serializer.open(null);
		deserializer.open(null);

		byte[] oriBytes = writeRecord(address, Address.getClassSchema());
		RowData rowData = deserializer.deserialize(oriBytes);
		byte[] serialized = serializer.serialize(rowData);
		assertArrayEquals(oriBytes, serialized);
	}
}
