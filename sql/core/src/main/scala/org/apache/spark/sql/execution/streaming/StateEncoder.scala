/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.streaming

import java.io.{ByteArrayOutputStream, Serializable}

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericDatumReader, GenericDatumWriter}
import org.apache.avro.io.{BinaryDecoder, BinaryEncoder, DecoderFactory, EncoderFactory}
import org.apache.commons.lang3.SerializationUtils

import org.apache.spark.sql.Encoder
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.{encoderFor, ExpressionEncoder}
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, SpecificInternalRow, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.core.avro.{AvroDeserializer, AvroOptions, AvroSerializer, SchemaConverters}
import org.apache.spark.sql.execution.streaming.{FastByteArrayOutputStream, ImplicitGroupingKeyTracker}
import org.apache.spark.sql.execution.streaming.state.StateStoreErrors
import org.apache.spark.sql.types.{BinaryType, StructType}

/**
 * Helper object providing APIs to encodes the grouping key, and user provided values
 * to Spark [[UnsafeRow]].
 */
class StateEncoder[S](valEnc: Encoder[S]) {
  /** Variables reused for conversions between object/binary and internal row */
  private val schemaForKeyRow: StructType = new StructType().add("key", BinaryType)
  private val schemaForValueRow: StructType = new StructType().add("value", BinaryType)
  private val valueRowEncoder = UnsafeProjection.create(schemaForValueRow)

  /** Variables reused for conversions between spark sql and object */
  private val valExpressionEnc = encoderFor(valEnc)
  private val objToRowSerializer = valExpressionEnc.createSerializer()
  private val rowToObjDeserializer = valExpressionEnc.resolveAndBind().createDeserializer()

  /** Variables reused for conversions between spark sql and avro */
  private val reuseRow = new UnsafeRow(valEnc.schema.fields.length)
  private var encoder: BinaryEncoder = _
  private val slowout = new ByteArrayOutputStream(128)
  private val out = new FastByteArrayOutputStream
  private var decoder: BinaryDecoder = _
  private var result: Any = _
  val valSchema: StructType = valEnc.schema
  val parser = new Schema.Parser()
  val schemaStr =
    """
      |{"namespace": "org.apache.spark.sql.execution.streaming.state",
      |  "type": "Egg",
      |  "name": "Egg",
      |  "fields": [
      |    {"name": "id", "type": "int"},
      |    {"name": "name", "type": "string"},
      |    {"name": "age", "type": "int"},
      |    {"name": "weight", "type": "double"}
      |  ]
      |}
      |""".stripMargin
  // dataType -> avroType
  val avroType: Schema = SchemaConverters.toAvroType(valSchema)
  /** Serializer */
  val avroSerializer = new AvroSerializer(valSchema, avroType, nullable = false)
  encoder = EncoderFactory.get().directBinaryEncoder(out, encoder)
  val writer = new GenericDatumWriter[Any](avroType)
  /** Deserializer */
  val avroOptions = AvroOptions(Map.empty)
  val avroDeserializer = new AvroDeserializer(avroType, valSchema,
    avroOptions.datetimeRebaseModeInRead, avroOptions.useStableIdForUnionType,
    avroOptions.stableIdPrefixForUnionType)
  val reader = new GenericDatumReader[Any](avroType)

  def encodeGroupingKey(stateName: String, keyExprEnc: ExpressionEncoder[Any]): UnsafeRow = {
    val keyOption = ImplicitGroupingKeyTracker.getImplicitKeyOption
    if (!keyOption.isDefined) {
      throw StateStoreErrors.implicitKeyNotFound(stateName)
    }

    val toRow = keyExprEnc.createSerializer()
    val keyByteArr = toRow
      .apply(keyOption.get).asInstanceOf[UnsafeRow].getBytes()

    val keyEncoder = UnsafeProjection.create(schemaForKeyRow)
    val keyRow = keyEncoder(InternalRow(keyByteArr))
    keyRow
  }

  def encodeValue(value: S): UnsafeRow = {
    val valueByteArr = SerializationUtils.serialize(value.asInstanceOf[Serializable])
    val valueRow = valueRowEncoder(InternalRow(valueByteArr))
    valueRow
  }

  def decodeValue(row: UnsafeRow): S = {
    SerializationUtils
      .deserialize(row.getBinary(0))
      .asInstanceOf[S]
  }

  def encodeValSparkSQL(value: S): UnsafeRow = {
    val objRow: InternalRow = objToRowSerializer.apply(value)
    val bytes = objRow.asInstanceOf[UnsafeRow].getBytes()
    val valRow = valueRowEncoder(InternalRow(bytes))
    valRow
  }

  def decodeValSparkSQL(row: UnsafeRow): S = {
    val bytes = row.getBinary(0)
    reuseRow.pointTo(bytes, bytes.length)
    val value = rowToObjDeserializer.apply(reuseRow)
    value
  }

  var objectRow: InternalRow = null
  var avroData: Any = null
  var avroBinary: Array[Byte] = null
  var internalRow: InternalRow = null
  var unsafeRow: UnsafeRow = null

  // 3250ms
  def encodeValToAvro(value: S): UnsafeRow = {
    // 350 ms
    if (objectRow == null || true) objectRow = objToRowSerializer.apply(value)

    /** The following parts are avro specific */
    // 750 ms
    if (avroData == null || true) avroData = avroSerializer.serialize(objectRow)
    // 900 ms
    if (avroBinary == null || true) {
      out.reset()
      writer.write(avroData, encoder)
      encoder.flush()
      // 700 ms
      // avro bytes
      if (avroBinary == null || true) avroBinary = out.toByteArray
    }
    // 400 ms
    /** Avro specific parts end here */
    // bytes -> InternalRow
    if (internalRow == null || true) {
      internalRow = new GenericInternalRow(Array[Any](avroBinary))
      // internalRow = InternalRow(avroBinary)
    }

    unsafeRow = valueRowEncoder(internalRow)
    unsafeRow
  }

  def decodeAvroToValue(row: UnsafeRow): S = {
    // InternalRow -> bytes
    val avroBytes = row.getBinary(0) // avoid copy

    /** The following parts are avro specific */
    decoder = DecoderFactory.get().binaryDecoder(avroBytes, 0, avroBytes.length, decoder)
    result = reader.read(result, decoder)
    val deserialized = avroDeserializer.deserialize(result)
    val deserializedRow = deserialized.get.asInstanceOf[SpecificInternalRow]
    /** Avro specific parts end here */

    rowToObjDeserializer.apply(deserializedRow)
  }
}
