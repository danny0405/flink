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

package org.apache.flink.formats.avro.registry.confluent.catalog;

import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/** Tests for {@link SchemaRegistryCatalog}. */
public class SchemaRegistryCatalogTest {
	private static SchemaRegistryCatalog CATALOG;

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	@BeforeClass
	public static void beforeClass() throws IOException, RestClientException {
		Map<String, String> kafkaOptions = new HashMap<>(1);
		kafkaOptions.put("properties.bootstrap.servers", "localhost:9092");

		String avroSchemaString = "" +
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
		AvroSchema avroSchema = new AvroSchema(avroSchemaString);
		MockSchemaRegistryClient client = new MockSchemaRegistryClient(
				Collections.singletonList(new AvroSchemaProvider()));
		client.register("address-value", avroSchema);
		CATALOG = SchemaRegistryCatalog.builder()
				.catalogName("myCatalog")
				.dbName("myDB")
				.kafkaOptions(kafkaOptions)
				.schemaRegistryURL("http://localhost:8081")
				.registryClient(client)
				.build();
		CATALOG.open();
	}

	@AfterClass
	public static void afterClass() {
		CATALOG.close();
		CATALOG = null;
	}

	@Test
	public void testListDatabases() {
		List<String> databases = CATALOG.listDatabases();
		assertThat(databases.size(), is(1));
		assertEquals("myDB", databases.get(0));
	}

	@Test
	public void testListTables() {
		List<String> tables = CATALOG.listTables("myDB");
		assertThat(tables.size(), is(1));
		assertEquals("address", tables.get(0));
	}

	@Test
	public void testGetTable() throws TableNotExistException {
		final ObjectPath objectPath = ObjectPath.fromString("myDB.address");
		assertThat(CATALOG.tableExists(objectPath), is(true));
		CatalogBaseTable table = CATALOG.getTable(objectPath);
		final String expectedSchema = "root\n" +
				" |-- num: INT\n" +
				" |-- street: STRING\n" +
				" |-- city: STRING\n" +
				" |-- state: STRING\n" +
				" |-- zip: STRING\n";
		assertThat(table.getSchema().toString(), is(expectedSchema));
		final String expectedOptions = "{properties.bootstrap.servers=localhost:9092, "
				+ "avro-sr.schema-registry.url=http://localhost:8081, "
				+ "avro-sr.schema-registry.subject=address-value, "
				+ "connector=kafka, "
				+ "format=avro-sr, "
				+ "topic=address, "
				+ "avro-sr.schema-string={\"type\":\"record\","
				+ "\"name\":\"Address\","
				+ "\"namespace\":\"org.apache.flink.formats.avro.generated\","
				+ "\"fields\":[{\"name\":\"num\",\"type\":\"int\"},"
				+ "{\"name\":\"street\",\"type\":\"string\"},"
				+ "{\"name\":\"city\",\"type\":\"string\"},"
				+ "{\"name\":\"state\",\"type\":\"string\"},"
				+ "{\"name\":\"zip\",\"type\":\"string\"}]}}";
		assertThat(table.getOptions().toString(), is(expectedOptions));
	}

	@Test
	public void testMissingKafkaOptions() {
		thrown.expect(ValidationException.class);
		thrown.expectMessage("Option properties.bootstrap.servers is required for Kafka,"
				+ " please configure through Builder.kafkaOptions.");

		MockSchemaRegistryClient client = new MockSchemaRegistryClient();
		CATALOG = SchemaRegistryCatalog.builder()
				.catalogName("myCatalog")
				.dbName("myDB")
				.kafkaOptions(Collections.emptyMap())
				.schemaRegistryURL("http://localhost:8081")
				.registryClient(client)
				.build();
	}
}
