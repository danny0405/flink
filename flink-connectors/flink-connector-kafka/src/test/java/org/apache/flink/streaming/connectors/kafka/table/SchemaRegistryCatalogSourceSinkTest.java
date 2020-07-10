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

package org.apache.flink.streaming.connectors.kafka.table;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.formats.avro.registry.confluent.catalog.SchemaRegistryCatalog;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.FactoryUtil;

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
import java.util.Map;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * Tests that table from the
 * {@link org.apache.flink.formats.avro.registry.confluent.catalog.SchemaRegistryCatalog}
 * can be translated to table source sink correctly.
 */
public class SchemaRegistryCatalogSourceSinkTest {

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
	public void testFindTableSource() throws TableNotExistException {
		final ObjectPath objectPath = ObjectPath.fromString("myDB.address");
		assertThat(CATALOG.tableExists(objectPath), is(true));
		DynamicTableSource source = FactoryUtil.createTableSource(
				CATALOG,
				ObjectIdentifier.of("myCatalog", "myDB", "address"),
				(CatalogTable) CATALOG.getTable(objectPath),
				new Configuration(),
				Thread.currentThread().getContextClassLoader());
		assertThat(source instanceof KafkaDynamicSource, is(true));
		KafkaDynamicSource kafkaDynamicSource = (KafkaDynamicSource) source;
		assertThat(kafkaDynamicSource.outputDataType.toString(),
				is("ROW<`num` INT, `street` STRING, `city` STRING, `state` STRING, `zip` STRING>"));
		assertThat(kafkaDynamicSource.topic, is("address"));
	}

	@Test
	public void testFindTableSink() throws TableNotExistException {
		final ObjectPath objectPath = ObjectPath.fromString("myDB.address");
		assertThat(CATALOG.tableExists(objectPath), is(true));
		DynamicTableSink sink = FactoryUtil.createTableSink(
				CATALOG,
				ObjectIdentifier.of("myCatalog", "myDB", "address"),
				(CatalogTable) CATALOG.getTable(objectPath),
				new Configuration(),
				Thread.currentThread().getContextClassLoader());
		assertThat(sink instanceof KafkaDynamicSink, is(true));
		KafkaDynamicSink kafkaDynamicSink = (KafkaDynamicSink) sink;
		assertThat(kafkaDynamicSink.consumedDataType.toString(),
				is("ROW<`num` INT, `street` STRING, `city` STRING, `state` STRING, `zip` STRING>"));
		assertThat(kafkaDynamicSink.topic, is("address"));
	}
}
