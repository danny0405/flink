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

package org.apache.flink.formats.avro.registry.confluent;

import org.apache.flink.formats.avro.AvroSerializationSchema;
import org.apache.flink.formats.avro.RegistryAvroRowDataSerializationSchema;
import org.apache.flink.formats.avro.SchemaCoder;
import org.apache.flink.table.types.logical.RowType;

/**
 * Serialization schema that serializes from row data to Avro binary format that uses
 * Confluent Schema Registry.
 */
public class ConfluentRegistryAvroRowDataSerializationSchema
		extends RegistryAvroRowDataSerializationSchema {

	private static final long serialVersionUID = 1964280000042807149L;

	private static final int DEFAULT_IDENTITY_MAP_CAPACITY = 1000;

	/**
	 * Creates a Avro serialization schema.
	 *
	 * @param avroSchema          Writer's Avro schema string
	 * @param schemaCoderProvider Provider for schema coder that writes the writer schema to
	 *                            Confluent Schema Registry
	 * @param rowType             The input row type
	 */
	private ConfluentRegistryAvroRowDataSerializationSchema(
			String avroSchema,
			SchemaCoder.SchemaCoderProvider schemaCoderProvider,
			RowType rowType) {
		super(avroSchema, schemaCoderProvider, rowType);
	}

	/**
	 * Creates {@link AvroSerializationSchema} that produces byte arrays that were generated from avro
	 * schema and writes the writer schema to Confluent Schema Registry.
	 *
	 * @param subject           Subject of schema registry to produce
	 * @param avroSchema        Schema that will be used for serialization
	 * @param schemaRegistryUrl URL of schema registry to connect
	 * @param rowType           The input row type
	 */
	public static ConfluentRegistryAvroRowDataSerializationSchema create(
			String subject,
			String avroSchema,
			String schemaRegistryUrl,
			RowType rowType) {
		return new ConfluentRegistryAvroRowDataSerializationSchema(
				avroSchema,
				new CachedSchemaCoderProvider(subject, schemaRegistryUrl, DEFAULT_IDENTITY_MAP_CAPACITY),
				rowType);
	}
}
