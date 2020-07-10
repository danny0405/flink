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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.avro.RegistryAvroRowDataDeserializationSchema;
import org.apache.flink.formats.avro.SchemaCoder;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import org.apache.avro.generic.GenericRecord;

/**
 * Deserialization schema that deserializes Avro binary format into row data
 * using {@link SchemaCoder} that uses Confluent Schema Registry.
 */
public class ConfluentRegistryAvroRowDataDeserializationSchema
		extends RegistryAvroRowDataDeserializationSchema {

	private static final long serialVersionUID = -9220674411689244126L;

	private static final int DEFAULT_IDENTITY_MAP_CAPACITY = 1000;

	/**
	 * Creates an Avro deserialization schema.
	 *
	 * @param avroSchema          Reader's Avro schema string
	 * @param rowType             The output row type
	 * @param typeInfo            The output type info
	 * @param schemaCoderProvider Provider for schema coder that reads writer schema from
	 *                            Confluent Schema Registry
	 */
	private ConfluentRegistryAvroRowDataDeserializationSchema(
			String avroSchema,
			SchemaCoder.SchemaCoderProvider schemaCoderProvider,
			RowType rowType,
			TypeInformation<RowData> typeInfo) {
		super(avroSchema, rowType, typeInfo, schemaCoderProvider);
	}

	/**
	 * Creates {@link ConfluentRegistryAvroRowDataDeserializationSchema} that produces {@link GenericRecord}
	 * using provided reader schema and looks up writer schema in Confluent Schema Registry.
	 *
	 * @param avroSchema Schema string of produced records
	 * @param url        Url of schema registry to connect
	 * @param rowType    The output row type
	 * @param typeInfo   The output type info
	 */
	public static ConfluentRegistryAvroRowDataDeserializationSchema create(
			String avroSchema,
			String url,
			RowType rowType,
			TypeInformation<RowData> typeInfo) {
		return create(avroSchema, url, DEFAULT_IDENTITY_MAP_CAPACITY, rowType, typeInfo);
	}

	/**
	 * Creates {@link ConfluentRegistryAvroRowDataDeserializationSchema} that produces {@link GenericRecord}
	 * using provided reader schema and looks up writer schema in Confluent Schema Registry.
	 *
	 * @param avroSchema Schema string of produced records
	 * @param url        Url of schema registry to connect
	 * @param rowType    The output row type
	 * @param typeInfo   The output type info
	 * @param identityMapCapacity Maximum number of cached schema versions (default: 1000)
	 */
	public static ConfluentRegistryAvroRowDataDeserializationSchema create(
			String avroSchema,
			String url,
			int identityMapCapacity,
			RowType rowType,
			TypeInformation<RowData> typeInfo) {
		return new ConfluentRegistryAvroRowDataDeserializationSchema(
				avroSchema,
				new CachedSchemaCoderProvider(url, identityMapCapacity),
				rowType,
				typeInfo);
	}
}
