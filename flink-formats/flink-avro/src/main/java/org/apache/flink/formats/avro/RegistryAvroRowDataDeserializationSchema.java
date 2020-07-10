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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.WrappingRuntimeException;

import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.DatumReader;

import java.io.IOException;
import java.util.Objects;

/**
 * Deserialization schema from Schema Registry Avro bytes to {@link RowData}.
 */
public class RegistryAvroRowDataDeserializationSchema extends AvroRowDataDeserializationSchema {

	private static final long serialVersionUID = 6007425293968694158L;

	/** Provider for schema coder. Used for initializing in each task. */
	private final SchemaCoder.SchemaCoderProvider schemaCoderProvider;

	/** Coder used for reading schema from incoming stream. */
	private transient SchemaCoder schemaCoder;

	/** Avro schema string used for deserialization. */
	private String avroSchema;

	/**
	 * Creates Schema Registry Avro deserialization schema that reads schema from input stream
	 * using provided {@link SchemaCoder}.
	 *
	 * @param avroSchema Avro schema string used for deserialization
	 * @param rowType    The logical type used to deserialize the data
	 * @param typeInfo   The TypeInformation
	 * @param provider   Schema provider that allows instantiation of {@link SchemaCoder}
	 */
	protected RegistryAvroRowDataDeserializationSchema(
			String avroSchema,
			RowType rowType,
			TypeInformation<RowData> typeInfo,
			SchemaCoder.SchemaCoderProvider provider) {
		super(rowType, typeInfo);
		this.avroSchema = avroSchema;
		this.schemaCoderProvider = provider;
	}

	@Override
	public void open(InitializationContext context) throws Exception {
		super.open(context);
		this.schemaCoder = schemaCoderProvider.get();
	}

	@Override
	public RowData deserialize(byte[] message) throws IOException {
		getInputStream().setBuffer(message);
		Schema writerSchema = schemaCoder.readSchema(getInputStream());

		DatumReader<IndexedRecord> datumReader = getDatumReader();

		datumReader.setSchema(writerSchema);

		IndexedRecord record = datumReader.read(getRecord(), getDecoder());
		return (RowData) getRuntimeConverter().convert(record);
	}

	@Override
	protected Schema generatesAvroSchema() {
		Preconditions.checkNotNull(avroSchema, "Avro schema must not be null.");
		try {
			return new Schema.Parser().parse(avroSchema);
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
		RegistryAvroRowDataDeserializationSchema that = (RegistryAvroRowDataDeserializationSchema) o;
		return schemaCoderProvider.equals(that.schemaCoderProvider);
	}

	@Override
	public int hashCode() {
		return Objects.hash(super.hashCode(), schemaCoderProvider);
	}
}
