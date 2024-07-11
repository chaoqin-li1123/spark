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

package org.apache.spark.sql.execution.python

import java.io.{DataInputStream, DataOutputStream}
import java.nio.channels.{Channels, ServerSocketChannel}

import scala.collection.mutable
import scala.jdk.CollectionConverters.IterableHasAsJava

import com.google.protobuf.ByteString
import org.apache.arrow.vector.{VarCharVector, VectorSchemaRoot}
import org.apache.arrow.vector.ipc.ArrowStreamWriter
import org.apache.arrow.vector.types.pojo.{Field, FieldType, Schema}
import org.apache.arrow.vector.types.pojo.ArrowType.Utf8
import org.apache.arrow.vector.util.Text
import sun.nio.ch.ChannelInputStream

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.streaming.{ImplicitGroupingKeyTracker, StatefulProcessorHandleImpl, StatefulProcessorHandleState}
import org.apache.spark.sql.execution.streaming.state.StateMessage.{HandleState, ListStateCall, StatefulProcessorHandleCall, StateRequest}
import org.apache.spark.sql.streaming.{ListState, ValueState}
import org.apache.spark.sql.util.ArrowUtils


class TransformWithStateInPandasStateServer(
    private val socketChannel: ServerSocketChannel,
    private val statefulProcessorHandle: StatefulProcessorHandleImpl)
  extends Runnable
  with Logging{

  private var inputStream: DataInputStream = _
  private var outputStream: DataOutputStream = _

  private val valueStates = mutable.HashMap[String, ValueState[String]]()
  private val listStates = mutable.HashMap[String, ListState[String]]()

  val stringList1 = List("apple1", "banana1", "cherry1", "date1", "elderberry1", "fig1")
  val iterator1: Iterator[String] = stringList1.iterator

  val stringList2 = List("apple2", "banana2", "cherry2")
  val iterator2: Iterator[String] = stringList2.iterator

  val iteratorMap: Map[String, Iterator[String]] = Map(
    "state1" -> iterator1,
    "state2" -> iterator2
  )

  var count = 0

  def run(): Unit = {
    logWarning(s"Waiting for connection from Python worker")
    val channel = socketChannel.accept()
    assert(channel.isConnected)
    logWarning(s"read/write on channel - ${channel.getLocalAddress}")

    inputStream = new DataInputStream(
      new ChannelInputStream(channel))
    outputStream = new DataOutputStream(
      Channels.newOutputStream(channel)
    )

    while (channel.isConnected &&
      statefulProcessorHandle.getHandleState != StatefulProcessorHandleState.CLOSED) {

      logWarning(s"reading the version")
      val version = inputStream.readInt()

      if (version != -1) {
        logWarning(s"version = ${version}")
        assert(version == 0)
        val messageLen = inputStream.readInt()
        logWarning(s"parsing a message of ${messageLen} bytes")

        val messageBytes = new Array[Byte](messageLen)
        inputStream.read(messageBytes)
        logWarning(s"read bytes = ${messageBytes.mkString("Array(", ", ", ")")}")

        val message = StateRequest.parseFrom(ByteString.copyFrom(messageBytes))

        logWarning(s"read message = $message")
        handleRequest(message)
        logWarning(s"flush output stream")

        outputStream.writeInt(0)
        outputStream.flush()
      }
    }

    logWarning(s"done from the state server thread")
  }

  private def handleRequest(message: StateRequest): Unit = {
    if (message.getMethodCase == StateRequest.MethodCase.STATEFULPROCESSORHANDLECALL) {
      val statefulProcessorHandleRequest = message.getStatefulProcessorHandleCall
      if (statefulProcessorHandleRequest.getMethodCase ==
        StatefulProcessorHandleCall.MethodCase.SETHANDLESTATE) {
        val requestedState = statefulProcessorHandleRequest.getSetHandleState.getState
        requestedState match {
          case HandleState.INITIALIZED =>
            logWarning(s"set handle state to Initialized")
            statefulProcessorHandle.setHandleState(StatefulProcessorHandleState.INITIALIZED)
          case HandleState.CLOSED =>
            logWarning(s"set handle state to closed")
            statefulProcessorHandle.setHandleState(StatefulProcessorHandleState.CLOSED)
          case _ =>
        }
      } else if (statefulProcessorHandleRequest.getMethodCase ==
        StatefulProcessorHandleCall.MethodCase.GETLISTSTATE) {
        val listStateName = statefulProcessorHandleRequest.getGetListState.getStateName
//        initializeState("ListState", listStateName)
        logWarning(s"get list state for $listStateName and schema" +
          s" ${statefulProcessorHandleRequest.getGetListState.getSchema}")
      }
    } else if (message.getMethodCase == StateRequest.MethodCase.LISTSTATECALL) {
      val listStateCall = message.getListStateCall
      val methodCase = listStateCall.getMethodCase
      if (methodCase == ListStateCall.MethodCase.GET) {
        logWarning("Handling list state get")
        val fields = List(
          new Field("stringColumn", FieldType.nullable(new Utf8()), null)
        )

        val mySchema = new Schema(fields.asJava)
        val allocator =
          ArrowUtils.rootAllocator.newChildAllocator(s"stdout writer for python",
            0, Long.MaxValue)
        val root = VectorSchemaRoot.create(mySchema, allocator)

        val vector = root.getVector("stringColumn").asInstanceOf[VarCharVector]
        root.allocateNew()

        logWarning(s"Handling count $count")
        if (count % 2 == 0) {
          vector.allocateNew(2)
          val localIterator1 = iteratorMap("state1")
          vector.setSafe(0, new Text(localIterator1.next()))
          vector.setSafe(1, new Text(localIterator1.next()))
          root.setRowCount(2)
        } else {
          val localIterator2 = iteratorMap("state2")
          vector.setSafe(0, new Text(localIterator2.next()))
          root.setRowCount(1)
        }
        count += 1
        val writer = new ArrowStreamWriter(root, null, outputStream)
        writer.writeBatch()
        writer.end()
        outputStream.flush()
//        writer.close()

        // Clean up
//        root.close()
//        allocator.close()
      } else {
        outputStream.writeInt(1)
      }
    }
  }

  def setHandleState(handleState: String): Unit = {
    logWarning(s"setting handle state to $handleState")
    if (handleState == "CREATED") {
      statefulProcessorHandle.setHandleState(StatefulProcessorHandleState.CREATED)
      outputStream.writeInt(0)
    } else if (handleState == "INITIALIZED") {
      statefulProcessorHandle.setHandleState(StatefulProcessorHandleState.INITIALIZED)
      outputStream.writeInt(0)
    } else if (handleState == "CLOSED") {
      statefulProcessorHandle.setHandleState(StatefulProcessorHandleState.CLOSED)
      outputStream.writeInt(0)
    } else {
      // fail
      outputStream.writeInt(1)
    }
  }

  private def valueState(stateName: String, parts: Array[String]): Unit = {
    val stateOption = valueStates.get(stateName)
    val operation = parts(2)

    if (stateOption.isEmpty) {
      outputStream.writeInt(1)
    } else if (operation == "get") {
      val state = stateOption.get
      val key = parts(3)

      ImplicitGroupingKeyTracker.setImplicitKey(key)
      val result = state.getOption()
      outputStream.writeInt(0)
      if (result.isDefined) {
        outputStream.writeInt(1)
        logWarning(s"Writing ${result.get} to output")
        outputStream.writeUTF(s"${result.get}\n")
      } else {
        outputStream.writeInt(0)
      }
    } else if (operation == "set") {
      val state = stateOption.get
      val key = parts(3)
      val newState = parts(4)
      logWarning(s"updating state for $key to $newState.")

      ImplicitGroupingKeyTracker.setImplicitKey(key)
      state.update(newState)
      logWarning(s"writing success to output")
      outputStream.writeInt(0)
    } else {
      outputStream.writeInt(1)
    }
  }

  private def performRequest(message: String): Unit = {
    if (message.nonEmpty) {
      val parts: Array[String] = message.split(':')
      if (parts(0) == "setHandleState") {
        assert(parts.length == 2)
        setHandleState(parts(1))
      } else if (parts(0) == "getState") {
        val stateType = parts(1)
        val stateName = parts(2)

        initializeState(stateType, stateName)
      } else {
        val stateType = parts(0)
        val stateName = parts(1)

        stateType match {
          case "ValueState" =>
            valueState(stateName, parts)
          case _ =>
            outputStream.writeInt(1)
        }
      }
    }
  }

  private def initializeState(stateType: String, stateName: String): Unit = {
    if (stateType == "ValueState") {
      val state = statefulProcessorHandle.getValueState[String](stateName)
      valueStates.put(stateName, state)
      outputStream.writeInt(0)
    } else if (stateType == "ListState") {
      val state = statefulProcessorHandle.getListState[String](stateName)
      listStates.put(stateName, state)
      outputStream.writeInt(0)
    } else {
      outputStream.writeInt(1)
    }
  }
}

object TransformWithStateInPandasStateServer {
  @volatile private var id = 0

  def allocateServerId(): Int = synchronized {
    id = id + 1
    return id
  }
}
