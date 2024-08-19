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

package org.apache.spark.sql.execution.streaming.state

import java.util.UUID

import scala.util.Random

import org.apache.hadoop.conf.Configuration
import org.scalatest.BeforeAndAfter

import org.apache.spark.SparkException
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.execution.streaming.{ImplicitGroupingKeyTracker, StatefulProcessorHandleImpl}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

/**
 * Class that adds tests for single value ValueState types used in arbitrary stateful
 * operators such as transformWithState
 */
case class TestClass(var id: Long, var name: String)

case class Chicken(num: Int)
case class Egg(id: Int, name: String, age: Int, weight: Double, chicken: Chicken)

class ValueStateSuite extends SharedSparkSession
  with BeforeAndAfter {

  before {
    StateStore.stop()
    require(!StateStore.isMaintenanceRunning)
  }

  after {
    StateStore.stop()
    require(!StateStore.isMaintenanceRunning)
  }

  import StateStoreTestsHelper._

  val schemaForKeyRow: StructType = new StructType().add("key", BinaryType)

  val schemaForValueRow: StructType = new StructType().add("value", BinaryType)

  private def newStoreProviderWithValueState(useColumnFamilies: Boolean):
    RocksDBStateStoreProvider = {
    newStoreProviderWithValueState(StateStoreId(newDir(), Random.nextInt(), 0),
      numColsPrefixKey = 0,
      useColumnFamilies = useColumnFamilies)
  }

  private def newStoreProviderWithValueState(
      storeId: StateStoreId,
      numColsPrefixKey: Int,
      sqlConf: SQLConf = SQLConf.get,
      conf: Configuration = new Configuration,
      useColumnFamilies: Boolean = false): RocksDBStateStoreProvider = {
    val provider = new RocksDBStateStoreProvider()
    provider.init(
      storeId, schemaForKeyRow, schemaForValueRow, numColsPrefixKey = numColsPrefixKey,
      useColumnFamilies,
      new StateStoreConf(sqlConf), conf)
    provider
  }

  private def tryWithProviderResource[T](
      provider: StateStoreProvider)(f: StateStoreProvider => T): T = {
    try {
      f(provider)
    } finally {
      provider.close()
    }
  }

  ignore("Implicit key operations") {
    tryWithProviderResource(newStoreProviderWithValueState(true)) { provider =>
      val store = provider.getStore(0)
      val handle = new StatefulProcessorHandleImpl(store, UUID.randomUUID(),
        Encoders.STRING.asInstanceOf[ExpressionEncoder[Any]])

      val stateName = "testState"
      val testState: ValueState[Long] = handle.getValueState[Long]("testState", Encoders.scalaLong)
      assert(ImplicitGroupingKeyTracker.getImplicitKeyOption.isEmpty)
      val ex = intercept[Exception] {
        testState.update(123)
      }

      assert(ex.isInstanceOf[SparkException])
      checkError(
        ex.asInstanceOf[SparkException],
        errorClass = "INTERNAL_ERROR_TWS",
        parameters = Map(
          "message" -> s"Implicit key not found in state store for stateName=$stateName"
        ),
        matchPVals = true
      )
      ImplicitGroupingKeyTracker.setImplicitKey("test_key")
      assert(ImplicitGroupingKeyTracker.getImplicitKeyOption.isDefined)
      testState.update(123)
      assert(testState.get() === 123)

      ImplicitGroupingKeyTracker.removeImplicitKey()
      assert(ImplicitGroupingKeyTracker.getImplicitKeyOption.isEmpty)

      val ex1 = intercept[Exception] {
        testState.update(123)
      }
      checkError(
        ex.asInstanceOf[SparkException],
        errorClass = "INTERNAL_ERROR_TWS",
        parameters = Map(
          "message" -> s"Implicit key not found in state store for stateName=$stateName"
        ),
        matchPVals = true
      )
    }
  }

  ignore("Value state operations for single & primitive instance") {
    tryWithProviderResource(newStoreProviderWithValueState(true)) { provider =>
      val store = provider.getStore(0)
      val handle = new StatefulProcessorHandleImpl(store, UUID.randomUUID(),
        Encoders.STRING.asInstanceOf[ExpressionEncoder[Any]])

      val testState: ValueState[Long] = handle.getValueState[Long]("testState", Encoders.scalaLong)
      ImplicitGroupingKeyTracker.setImplicitKey("test_key")
      testState.update(123)
      assert(testState.get() === 123)
      testState.remove()
      assert(!testState.exists())
      assert(testState.get() === null)

      testState.update(456)
      assert(testState.get() === 456)
      assert(testState.get() === 456)
      testState.update(123)
      assert(testState.get() === 123)

      testState.remove()
      assert(!testState.exists())
      assert(testState.get() === null)
    }
  }

  ignore("Value state operations for multiple instances") {
    tryWithProviderResource(newStoreProviderWithValueState(true)) { provider =>
      val store = provider.getStore(0)
      val handle = new StatefulProcessorHandleImpl(store, UUID.randomUUID(),
        Encoders.STRING.asInstanceOf[ExpressionEncoder[Any]])

      val testState1: ValueState[Long] = handle.getValueState[Long]("testState1",
        Encoders.scalaLong)
      val testState2: ValueState[Long] = handle.getValueState[Long]("testState2",
        Encoders.scalaLong)
      ImplicitGroupingKeyTracker.setImplicitKey("test_key")
      testState1.update(123)
      assert(testState1.get() === 123)
      testState1.remove()
      assert(!testState1.exists())
      assert(testState1.get() === null)

      testState2.update(456)
      assert(testState2.get() === 456)
      testState2.remove()
      assert(!testState2.exists())
      assert(testState2.get() === null)

      testState1.update(456)
      assert(testState1.get() === 456)
      assert(testState1.get() === 456)
      testState1.update(123)
      assert(testState1.get() === 123)

      testState2.update(123)
      assert(testState2.get() === 123)
      assert(testState2.get() === 123)
      testState2.update(456)
      assert(testState2.get() === 456)

      testState1.remove()
      assert(!testState1.exists())
      assert(testState1.get() === null)

      testState2.remove()
      assert(!testState2.exists())
      assert(testState2.get() === null)
    }
  }

  ignore("colFamily with HDFSBackedStateStoreProvider should fail") {
    val storeId = StateStoreId(newDir(), Random.nextInt(), 0)
    val provider = new HDFSBackedStateStoreProvider()
    val storeConf = new StateStoreConf(new SQLConf())
    val ex = intercept[StateStoreMultipleColumnFamiliesNotSupportedException] {
      provider.init(
        storeId, keySchema, valueSchema, 0, useColumnFamilies = true,
        storeConf, new Configuration)
    }
    checkError(
      ex,
      errorClass = "UNSUPPORTED_FEATURE.STATE_STORE_MULTIPLE_COLUMN_FAMILIES",
      parameters = Map(
        "stateStoreProvider" -> "HDFSStateStoreProvider"
      ),
      matchPVals = true
    )
  }

  ignore("Value state operations for case class instances") {
    tryWithProviderResource(newStoreProviderWithValueState(true)) { provider =>
      val store = provider.getStore(0)
      val handle = new StatefulProcessorHandleImpl(store, UUID.randomUUID(),
        Encoders.STRING.asInstanceOf[ExpressionEncoder[Any]])

      val testState: ValueState[TestClass] = handle.getValueState[TestClass]("testState",
        Encoders.product[TestClass], SerializationType.AVRO)
      ImplicitGroupingKeyTracker.setImplicitKey("test_key")
      testState.update(TestClass(1, "testcase1"))
      assert(testState.get().equals(new TestClass(1, "testcase1")))
      testState.remove()
      assert(!testState.exists())
      assert(testState.get() === null)

      testState.update(TestClass(2, "testcase2"))
      assert(testState.get() === TestClass(2, "testcase2"))
      testState.update(TestClass(3, "testcase3"))
      assert(testState.get() === TestClass(3, "testcase3"))

      testState.remove()
      assert(!testState.exists())
      assert(testState.get() === null)
    }
  }

  ignore("Value state operations for POJO instances") {
    tryWithProviderResource(newStoreProviderWithValueState(true)) { provider =>
      val store = provider.getStore(0)
      val handle = new StatefulProcessorHandleImpl(store, UUID.randomUUID(),
        Encoders.STRING.asInstanceOf[ExpressionEncoder[Any]])

      val testState: ValueState[Person] = handle.getValueState[Person]("testState",
        Encoders.bean(classOf[Person]), SerializationType.AVRO)
      ImplicitGroupingKeyTracker.setImplicitKey("test_key")
      testState.update(new Person("testcase1", 1))
      assert(testState.get().equals(new Person("testcase1", 1)))
      testState.remove()
      assert(!testState.exists())
      assert(testState.get() === null)

      testState.update(new Person("testcase2", 2))
      assert(testState.get().equals(new Person("testcase2", 2)))
      testState.update(new Person("testcase3", 3))
      assert(testState.get().equals(new Person("testcase3", 3)))

      testState.remove()
      assert(!testState.exists())
      assert(testState.get() === null)
    }
  }

  test("avro encode case class") {
    val valEncoder = Encoders.product[Egg]
    val stateEncoder = new StateEncoder[Egg](valEncoder)

    val startTime = System.nanoTime()
    for (i <- 1 to 10000000) {
      val egg = Egg(i, "egghjdjhdsfhkjdf-1", 34, 24.56, Chicken(i))
      val row = stateEncoder.encodeValToAvro(egg)
    }
    val endTime = System.nanoTime()
    val elapsed = {
      (endTime - startTime) / 1.0e6
    }
    println(s"panda avro encode take $elapsed ms case class")
  }

  test("sql encode case class") {
    val valEncoder = Encoders.product[Egg]
    val stateEncoder = new StateEncoder[Egg](valEncoder)

    val startTime = System.nanoTime()
    for (i <- 1 to 10000000) {
      val egg = Egg(1, "egghjdjhdsfhkjdf-1", 34, 24.56, Chicken(3))
      val row = stateEncoder.encodeValSparkSQL(egg)
    }
    val endTime = System.nanoTime()
    val elapsed = {
      (endTime - startTime) / 1.0e6 // 300 nano second
    }
    println(s"panda sql encode take $elapsed ms case class")
  }

  test("sql encode double") {
    val valEncoder = Encoders.DOUBLE
    val stateEncoder = new StateEncoder[java.lang.Double](valEncoder)

    val startTime = System.nanoTime()
    for (i <- 1 to 10000000) {
      val row = stateEncoder.encodeValSparkSQL(12.3 + i)
    }
    val endTime = System.nanoTime()
    val elapsed = {
      (endTime - startTime) / 1.0e6
    }
    println(s"panda sql encode take $elapsed ms double")
  }

  test("avro encode double") {
    val valEncoder = Encoders.DOUBLE
    val stateEncoder = new StateEncoder[java.lang.Double](valEncoder)

    val startTime = System.nanoTime()
    for (i <- 1 to 10000000) {
      val row = stateEncoder.encodeValSparkSQL(12.3 + i)
    }
    val endTime = System.nanoTime()
    val elapsed = {
      (endTime - startTime) / 1.0e6
    }
    println(s"panda avro encode take $elapsed ms double")
  }

  test("rocksdb put") {
    tryWithProviderResource(newStoreProviderWithValueState(true)) { provider =>
      val store = provider.getStore(0)
      val valEncoder = Encoders.product[Egg]
      val stateEncoder = new StateEncoder[Egg](valEncoder)
      val egg = Egg(1, "egghjdjhdsfhkjdf-1", 34, 24.56, Chicken(3))
      val row = stateEncoder.encodeValSparkSQL(egg)

      val startTime = System.nanoTime()
      for (i <- 1 to 10000000) {
        store.put(row, row) // 1500 nano second
      }
      val endTime = System.nanoTime()
      val elapsed = {
        (endTime - startTime) / 1.0e6
      }
      println(s"rocksdb put take $elapsed ms")
    }
  }

  test("internal row") {
    tryWithProviderResource(newStoreProviderWithValueState(true)) { provider =>
      val arr = Array[Byte](1, 2, 3, 2, 3, 5, 6, 7, 8, 90, 2, 28)
      val row = new GenericInternalRow(Array[Any](arr))
      val startTime = System.nanoTime()
      for (i <- 1 to 10000000) {
        val row = InternalRow(arr)
        // val egg = Egg(i, "egghjdjhdsfhkjdf-1", 34, 24.56, Chicken(3))
      }
      val endTime = System.nanoTime()
      val elapsed = {
        (endTime - startTime) / 1.0e6
      }
      println(s"create internal row take $elapsed ms")
    }
  }
}
