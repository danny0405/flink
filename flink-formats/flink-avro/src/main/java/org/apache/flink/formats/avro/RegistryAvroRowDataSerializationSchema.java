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

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.WrappingRuntimeException;

import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Encoder;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Objects;

/**
 * Serialization schema that serializes to Schema Registry Avro binary format.
 */
public class RegistryAvroRowDataSerializationSchema extends AvroRowDataSerializationSchema {

	private static final long serialVersionUID = -3406915403840308113L;

	/** Provider for schema coder. Used for initializing in each task. */
	private final SchemaCoder.SchemaCoderProvider schemaCoderProvider;

	protected SchemaCoder schemaCoder;

	/** Avro schema string used for serialization. */
	private String avroSchemaString;

	/**
	 * Creates an Avro serialization schema with given record type and schema coder provider.
	 *
	 * @param avroSchemaString    Avro schema string used for serialization
	 * @param schemaCoderProvider Schema provider that allows instantiation of {@link SchemaCoder}
	 * @param rowType             Row type of the input record
	 */
	protected RegistryAvroRowDataSerializationSchema(
			String avroSchemaString,
			SchemaCoder.SchemaCoderProvider schemaCoderProvider,
			RowType rowType) {
		super(rowType);
		this.avroSchemaString = avroSchemaString;
		this.schemaCoderProvider = schemaCoderProvider;
	}

	@Override
	public void open(InitializationContext context) throws Exception {
		super.open(context);
		this.schemaCoder = this.schemaCoderProvider.get();
	}

	@Override
	public byte[] serialize(RowData row) {
		try {
			ByteArrayOutputStream outputStream = getOutputStream();
			outputStream.reset();
			Encoder encoder = getEncoder();
			Schema schema = getSchema();
			schemaCoder.writeSchema(schema, outputStream);
			final GenericRecord record = (GenericRecord) getRuntimeConverter().convert(schema, row);
			getDatumWriter().write(record, encoder);
			encoder.flush();
			return outputStream.toByteArray();
		} catch (IOException e) {
			throw new WrappingRuntimeException("Failed to serialize row to schema registry avro.", e);
		}
	}

	@Override
	protected Schema generatesAvroSchema() {
		Preconditions.checkNotNull(avroSchemaString, "Avro schema must not be null.");
		try {
			return new Schema.Parser().parse(avroSchemaString);
		} catch (SchemaParseException e) {
			throw new WrappingRuntimeException("Could not parse Avro schema string.", e);
		}
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		if (!super.equals(o)) {
			return false;
		}
		RegistryAvroRowDataSerializationSchema that = (RegistryAvroRowDataSerializationSchema) o;
		return schemaCoderProvider.equals(that.schemaCoderProvider) &&
				avroSchemaString.equals(that.avroSchemaString);
	}

	@Override
	public int hashCode() {
		return Objects.hash(super.hashCode(), schemaCoderProvider, avroSchemaString);
	}
}
