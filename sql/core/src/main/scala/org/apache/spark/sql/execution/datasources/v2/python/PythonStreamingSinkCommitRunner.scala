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

package org.apache.spark.sql.execution.datasources.v2.python

import java.io.{BufferedInputStream, BufferedOutputStream, DataInputStream, DataOutputStream}

import scala.jdk.CollectionConverters._

import org.apache.spark.SparkEnv
import org.apache.spark.api.python.{PythonFunction, PythonWorker, PythonWorkerFactory, PythonWorkerUtils, SpecialLengths}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.BUFFER_SIZE
import org.apache.spark.internal.config.Python.{PYTHON_AUTH_SOCKET_TIMEOUT, PYTHON_USE_DAEMON}
import org.apache.spark.sql.connector.write.WriterCommitMessage
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.types.StructType

class PythonStreamingSinkCommitRunner(
    func: PythonFunction,
    schema: StructType,
    overwrite: Boolean) extends Logging {
  val workerModule: String = "pyspark.sql.worker.python_streaming_sink_runner"

  private val conf = SparkEnv.get.conf
  protected val bufferSize: Int = conf.get(BUFFER_SIZE)
  protected val authSocketTimeout = conf.get(PYTHON_AUTH_SOCKET_TIMEOUT)

  private val envVars: java.util.Map[String, String] = func.envVars
  private val pythonExec: String = func.pythonExec
  private var pythonWorker: Option[PythonWorker] = None
  private var pythonWorkerFactory: Option[PythonWorkerFactory] = None
  protected val pythonVer: String = func.pythonVer

  private var dataOut: DataOutputStream = null
  private var dataIn: DataInputStream = null

  /**
   * Initializes the Python worker for running the streaming source.
   */
  def init(): Unit = {
    logInfo(s"Initializing Python runner pythonExec: $pythonExec")
    val env = SparkEnv.get

    val localdir = env.blockManager.diskBlockManager.localDirs.map(f => f.getPath()).mkString(",")
    envVars.put("SPARK_LOCAL_DIRS", localdir)

    envVars.put("SPARK_AUTH_SOCKET_TIMEOUT", authSocketTimeout.toString)
    envVars.put("SPARK_BUFFER_SIZE", bufferSize.toString)

    val prevConf = conf.get(PYTHON_USE_DAEMON)
    conf.set(PYTHON_USE_DAEMON, false)
    try {
      val workerFactory =
        new PythonWorkerFactory(pythonExec, workerModule, envVars.asScala.toMap)
      val (worker: PythonWorker, _) = workerFactory.createSimpleWorker(blockingMode = true)
      pythonWorker = Some(worker)
      pythonWorkerFactory = Some(workerFactory)
    } finally {
      conf.set(PYTHON_USE_DAEMON, prevConf)
    }

    val stream = new BufferedOutputStream(
      pythonWorker.get.channel.socket().getOutputStream, bufferSize)
    dataOut = new DataOutputStream(stream)

    PythonWorkerUtils.writePythonVersion(pythonVer, dataOut)

    val pythonIncludes = func.pythonIncludes.asScala.toSet
    PythonWorkerUtils.writeSparkFiles(Some("streaming_job"), pythonIncludes, dataOut)

    // Send the user function to python process
    PythonWorkerUtils.writePythonFunction(func, dataOut)

    PythonWorkerUtils.writeUTF(schema.json, dataOut)

    dataOut.writeBoolean(overwrite)

    dataOut.flush()

    dataIn = new DataInputStream(
      new BufferedInputStream(pythonWorker.get.channel.socket().getInputStream, bufferSize))

    val initStatus = dataIn.readInt()
    if (initStatus == SpecialLengths.PYTHON_EXCEPTION_THROWN) {
      val msg = PythonWorkerUtils.readUTF(dataIn)
      throw QueryCompilationErrors.pythonDataSourceError(
        action = "plan", tpe = "initialize sink", msg = msg)
    }
  }

  init()

  def commitOrAbort(
      messages: Array[WriterCommitMessage],
      batchId: Long,
      abort: Boolean): Unit = {
    dataOut.writeInt(messages.length)
    messages.foreach { message =>
      // Commit messages can be null if there are task failures.
      if (message == null) {
        dataOut.writeInt(SpecialLengths.NULL)
      } else {
        PythonWorkerUtils.writeBytes(
          message.asInstanceOf[PythonWriterCommitMessage].pickledMessage, dataOut)
      }
    }
    dataOut.writeLong(batchId)
    dataOut.writeBoolean(abort)
    dataOut.flush()
    val status = dataIn.readInt()
    if (status == SpecialLengths.PYTHON_EXCEPTION_THROWN) {
      val msg = PythonWorkerUtils.readUTF(dataIn)
      throw QueryCompilationErrors.pythonDataSourceError(
        action = "plan", tpe = "initialize source", msg = msg)
    }

  }
}
