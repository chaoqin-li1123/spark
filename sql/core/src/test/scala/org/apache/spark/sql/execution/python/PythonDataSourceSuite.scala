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

import org.apache.spark.sql.{AnalysisException, IntegratedUDFTestUtils, QueryTest, Row}
import org.apache.spark.sql.execution.datasources.v2.{BatchScanExec, DataSourceV2ScanRelation}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class PythonDataSourceSuite extends QueryTest with SharedSparkSession {
  import IntegratedUDFTestUtils._

  private def dataSourceName = "SimpleDataSource"
  private def simpleDataSourceReaderScript: String =
    """
      |from pyspark.sql.datasource import DataSourceReader, InputPartition
      |class SimpleDataSourceReader(DataSourceReader):
      |    def partitions(self):
      |        return [InputPartition(i) for i in range(2)]
      |    def read(self, partition):
      |        yield (0, partition.value)
      |        yield (1, partition.value)
      |        yield (2, partition.value)
      |""".stripMargin

  private def simpleDataStreamReaderScript: String =
    """
      |from pyspark.sql.datasource import DataStreamReader, InputPartition, DataStreamOffset
      |
      |class SimpleDataStreamReader(DataStreamReader):
      |    def latest_offset(self):
      |        return DataStreamOffset(0)
      |    def partitions(self, start: DataStreamOffset, end: DataStreamOffset):
      |        return [InputPartition(i) for i in range(start.value, end.value + 1)]
      |    def read(self, partition):
      |        yield (0, partition.value)
      |        yield (1, partition.value)
      |        yield (2, partition.value)
      |    def offset_to_json(self, offset):
      |        return "SimpleDataStreamOffset"
      |    def json_to_offset(self, json):
      |        return DataStreamOffset(0)
      |""".stripMargin

  test("simple data source") {
    assume(shouldTestPandasUDFs)
    val dataSourceScript =
      s"""
        |from pyspark.sql.datasource import DataSource
        |$simpleDataSourceReaderScript
        |
        |class $dataSourceName(DataSource):
        |    def reader(self, schema):
        |        return SimpleDataSourceReader()
        |""".stripMargin
    val schema = StructType.fromDDL("id INT, partition INT")
    val dataSource = createUserDefinedPythonDataSource(
      name = dataSourceName, pythonScript = dataSourceScript)
    spark.dataSource.registerPython(dataSourceName, dataSource)
    val df = spark.read.format(dataSourceName).schema(schema).load()
    assert(df.rdd.getNumPartitions == 2)
    val plan = df.queryExecution.optimizedPlan
    plan match {
      case s: DataSourceV2ScanRelation
        if s.relation.table.getClass.toString.contains("PythonTable") =>
      case _ => fail(s"Plan did not match the expected pattern. Actual plan:\n$plan")
    }
    checkAnswer(df, Seq(Row(0, 0), Row(0, 1), Row(1, 0), Row(1, 1), Row(2, 0), Row(2, 1)))
  }

  test("simple data stream source") {
    assume(shouldTestPandasUDFs)
    val dataSourceScript =
      s"""
         |from pyspark.sql.datasource import DataSource
         |$simpleDataStreamReaderScript
         |
         |class $dataSourceName(DataSource):
         |    def stream_reader(self, schema):
         |        return SimpleDataStreamReader()
         |""".stripMargin
    val inputSchema = StructType.fromDDL("input BINARY")
    val schema = StructType.fromDDL("id INT, partition INT")
    val dataSource = createUserDefinedPythonDataSource(
      name = dataSourceName, pythonScript = dataSourceScript)
    val dataSourceInPython = dataSource
      .createDataSourceInPython(dataSourceName, CaseInsensitiveStringMap.empty, Some(schema))
    val func = dataSource.createPythonFunction(
      dataSourceInPython.dataSource)
    val streamingSourceRunner =
      new StreamingSourcePythonRunner(func, inputSchema, schema)
    streamingSourceRunner.init()
    // assert(streamingSourceRunner.latestOffset() == "SimpleDataStreamOffset")
    assert(streamingSourceRunner
      .partitions("SimpleDataStreamOffset", "SimpleDataStreamOffset") == 2)
    streamingSourceRunner.stop()
  }

  test("simple data source with string schema") {
    assume(shouldTestPandasUDFs)
    val dataSourceScript =
      s"""
         |from pyspark.sql.datasource import DataSource, DataSourceReader
         |$simpleDataSourceReaderScript
         |
         |class $dataSourceName(DataSource):
         |    def schema(self) -> str:
         |        return "id INT, partition INT"
         |
         |    def reader(self, schema):
         |        return SimpleDataSourceReader()
         |""".stripMargin
    val dataSource = createUserDefinedPythonDataSource(dataSourceName, dataSourceScript)
    spark.dataSource.registerPython(dataSourceName, dataSource)
    val df = spark.read.format(dataSourceName).load()
    checkAnswer(df, Seq(Row(0, 0), Row(0, 1), Row(1, 0), Row(1, 1), Row(2, 0), Row(2, 1)))
  }

  test("simple data source with StructType schema") {
    assume(shouldTestPandasUDFs)
    val dataSourceScript =
      s"""
         |from pyspark.sql.datasource import DataSource, DataSourceReader
         |from pyspark.sql.types import IntegerType, StructType, StructField
         |$simpleDataSourceReaderScript
         |
         |class $dataSourceName(DataSource):
         |    def schema(self) -> str:
         |        return StructType([
         |            StructField("id", IntegerType()),
         |            StructField("partition", IntegerType())
         |        ])
         |
         |    def reader(self, schema):
         |        return SimpleDataSourceReader()
         |""".stripMargin
    val dataSource = createUserDefinedPythonDataSource(dataSourceName, dataSourceScript)
    spark.dataSource.registerPython(dataSourceName, dataSource)
    val df = spark.read.format(dataSourceName).load()
    checkAnswer(df, Seq(Row(0, 0), Row(0, 1), Row(1, 0), Row(1, 1), Row(2, 0), Row(2, 1)))
  }

  test("data source with invalid schema") {
    assume(shouldTestPandasUDFs)
    val dataSourceScript =
      s"""
         |from pyspark.sql.datasource import DataSource, DataSourceReader
         |$simpleDataSourceReaderScript
         |
         |class $dataSourceName(DataSource):
         |    def schema(self) -> str:
         |        return "INT"
         |
         |    def reader(self, schema):
         |        return SimpleDataSourceReader()
         |""".stripMargin
    val dataSource = createUserDefinedPythonDataSource(dataSourceName, dataSourceScript)
    spark.dataSource.registerPython(dataSourceName, dataSource)
    checkError(
      exception = intercept[AnalysisException](spark.read.format(dataSourceName).load()),
      errorClass = "INVALID_SCHEMA.NON_STRUCT_TYPE",
      parameters = Map("inputSchema" -> "INT", "dataType" -> "\"INT\""))
  }

  test("register data source") {
    assume(shouldTestPandasUDFs)
    val dataSourceScript =
      s"""
         |from pyspark.sql.datasource import DataSource, DataSourceReader
         |$simpleDataSourceReaderScript
         |
         |class $dataSourceName(DataSource):
         |    def schema(self) -> str:
         |        return "id INT, partition INT"
         |
         |    def reader(self, schema):
         |        return SimpleDataSourceReader()
         |""".stripMargin

    val dataSource = createUserDefinedPythonDataSource(dataSourceName, dataSourceScript)
    spark.dataSource.registerPython(dataSourceName, dataSource)
    assert(spark.sessionState.dataSourceManager.dataSourceExists(dataSourceName))
    checkAnswer(
      spark.read.format(dataSourceName).load(),
      Seq(Row(0, 0), Row(0, 1), Row(1, 0), Row(1, 1), Row(2, 0), Row(2, 1)))

    // Should be able to override an already registered data source.
    val newScript =
      s"""
         |from pyspark.sql.datasource import DataSource, DataSourceReader
         |class SimpleDataSourceReader(DataSourceReader):
         |    def read(self, partition):
         |        yield (0, )
         |
         |class $dataSourceName(DataSource):
         |    def schema(self) -> str:
         |        return "id INT"
         |
         |    def reader(self, schema):
         |        return SimpleDataSourceReader()
         |""".stripMargin
    val newDataSource = createUserDefinedPythonDataSource(dataSourceName, newScript)
    spark.dataSource.registerPython(dataSourceName, newDataSource)
    assert(spark.sessionState.dataSourceManager.dataSourceExists(dataSourceName))
    checkAnswer(
      spark.read.format(dataSourceName).load(),
      Seq(Row(0)))
  }

  test("load data source") {
    assume(shouldTestPandasUDFs)
    val dataSourceScript =
      s"""
         |from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition
         |import json
         |
         |class SimpleDataSourceReader(DataSourceReader):
         |    def __init__(self, options):
         |        self.options = options
         |
         |    def partitions(self):
         |        if "paths" in self.options:
         |            paths = json.loads(self.options["paths"])
         |        elif "path" in self.options:
         |            paths = [self.options["path"]]
         |        else:
         |            paths = []
         |        return [InputPartition(p) for p in paths]
         |
         |    def read(self, part):
         |        if part is not None:
         |            assert isinstance(part, InputPartition)
         |            yield (part.value, 1)
         |        else:
         |            yield (part, 1)
         |
         |class $dataSourceName(DataSource):
         |    @classmethod
         |    def name(cls) -> str:
         |        return "test"
         |
         |    def schema(self) -> str:
         |        return "id STRING, value INT"
         |
         |    def reader(self, schema):
         |        return SimpleDataSourceReader(self.options)
         |""".stripMargin
    val dataSource = createUserDefinedPythonDataSource(dataSourceName, dataSourceScript)
    spark.dataSource.registerPython("test", dataSource)
    checkAnswer(spark.read.format("test").load(), Seq(Row(null, 1)))
    checkAnswer(spark.read.format("test").load("1"), Seq(Row("1", 1)))
    checkAnswer(spark.read.format("test").load("1", "2"), Seq(Row("1", 1), Row("2", 1)))

    withTable("tblA") {
      sql("CREATE TABLE tblA USING test")
      // The path will be the actual temp path.
      checkAnswer(spark.table("tblA").selectExpr("value"), Seq(Row(1)))
    }
  }

  test("reader not implemented") {
    assume(shouldTestPandasUDFs)
    val dataSourceScript =
       s"""
        |from pyspark.sql.datasource import DataSource, DataSourceReader
        |class $dataSourceName(DataSource):
        |    pass
        |""".stripMargin
    val schema = StructType.fromDDL("id INT, partition INT")
    val dataSource = createUserDefinedPythonDataSource(
      name = dataSourceName, pythonScript = dataSourceScript)
    spark.dataSource.registerPython(dataSourceName, dataSource)
    val err = intercept[AnalysisException] {
      spark.read.format(dataSourceName).schema(schema).load().collect()
    }
    assert(err.getErrorClass == "PYTHON_DATA_SOURCE_FAILED_TO_PLAN_IN_PYTHON")
    assert(err.getMessage.contains("PYTHON_DATA_SOURCE_METHOD_NOT_IMPLEMENTED"))
  }

  test("error creating reader") {
    assume(shouldTestPandasUDFs)
    val dataSourceScript =
      s"""
        |from pyspark.sql.datasource import DataSource
        |class $dataSourceName(DataSource):
        |    def reader(self, schema):
        |        raise Exception("error creating reader")
        |""".stripMargin
    val schema = StructType.fromDDL("id INT, partition INT")
    val dataSource = createUserDefinedPythonDataSource(
      name = dataSourceName, pythonScript = dataSourceScript)
    spark.dataSource.registerPython(dataSourceName, dataSource)
    val err = intercept[AnalysisException] {
      spark.read.format(dataSourceName).schema(schema).load().collect()
    }
    assert(err.getErrorClass == "PYTHON_DATA_SOURCE_FAILED_TO_PLAN_IN_PYTHON")
    assert(err.getMessage.contains("PYTHON_DATA_SOURCE_CREATE_ERROR"))
    assert(err.getMessage.contains("error creating reader"))
  }

  test("data source assertion error") {
    assume(shouldTestPandasUDFs)
    val dataSourceScript =
      s"""
        |class $dataSourceName:
        |   def __init__(self, options):
        |       ...
        |""".stripMargin
    val schema = StructType.fromDDL("id INT, partition INT")
    val dataSource = createUserDefinedPythonDataSource(
      name = dataSourceName, pythonScript = dataSourceScript)
    spark.dataSource.registerPython(dataSourceName, dataSource)
    val err = intercept[AnalysisException] {
      spark.read.format(dataSourceName).schema(schema).load().collect()
    }
    assert(err.getErrorClass == "PYTHON_DATA_SOURCE_FAILED_TO_PLAN_IN_PYTHON")
    assert(err.getMessage.contains("PYTHON_DATA_SOURCE_TYPE_MISMATCH"))
    assert(err.getMessage.contains("PySparkAssertionError"))
  }

  test("data source read with custom partitions") {
    assume(shouldTestPandasUDFs)
    val dataSourceScript =
      s"""
         |from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition
         |class RangePartition(InputPartition):
         |    def __init__(self, start, end):
         |        self.start = start
         |        self.end = end
         |
         |class SimpleDataSourceReader(DataSourceReader):
         |    def partitions(self):
         |        return [RangePartition(1, 2), RangePartition(3, 4)]
         |
         |    def read(self, partition: RangePartition):
         |        start, end = partition.start, partition.end
         |        for i in range(start, end):
         |            yield (i, )
         |
         |class $dataSourceName(DataSource):
         |    def schema(self) -> str:
         |        return "id INT"
         |
         |    def reader(self, schema):
         |        return SimpleDataSourceReader()
         |""".stripMargin
    val dataSource = createUserDefinedPythonDataSource(dataSourceName, dataSourceScript)
    spark.dataSource.registerPython(dataSourceName, dataSource)
    val df = spark.read.format(dataSourceName).load()
    checkAnswer(df, Seq(Row(1), Row(3)))
  }

  test("data source read with empty partitions") {
    assume(shouldTestPandasUDFs)
    val dataSourceScript =
      s"""
         |from pyspark.sql.datasource import DataSource, DataSourceReader
         |class SimpleDataSourceReader(DataSourceReader):
         |    def partitions(self):
         |        return []
         |
         |    def read(self, partition):
         |        if partition is None:
         |            yield ("success", )
         |        else:
         |            yield ("failed", )
         |
         |class $dataSourceName(DataSource):
         |    def schema(self) -> str:
         |        return "status STRING"
         |
         |    def reader(self, schema):
         |        return SimpleDataSourceReader()
         |""".stripMargin
    val dataSource = createUserDefinedPythonDataSource(dataSourceName, dataSourceScript)
    spark.dataSource.registerPython(dataSourceName, dataSource)
    val df = spark.read.format(dataSourceName).load()
    checkAnswer(df, Row("success"))
  }

  test("data source read with invalid partitions") {
    assume(shouldTestPandasUDFs)
    val reader1 =
      s"""
         |class SimpleDataSourceReader(DataSourceReader):
         |    def partitions(self):
         |        return 1
         |    def read(self, partition):
         |        ...
         |""".stripMargin

    val reader2 =
      s"""
         |class SimpleDataSourceReader(DataSourceReader):
         |    def partitions(self):
         |        return [1, 2]
         |    def read(self, partition):
         |        ...
         |""".stripMargin

    val reader3 =
      s"""
         |class SimpleDataSourceReader(DataSourceReader):
         |    def partitions(self):
         |        raise Exception("error")
         |    def read(self, partition):
         |        ...
         |""".stripMargin

    Seq(reader1, reader2, reader3).foreach { readerScript =>
      val dataSourceScript =
        s"""
           |from pyspark.sql.datasource import DataSource, DataSourceReader
           |$readerScript
           |
           |class $dataSourceName(DataSource):
           |    def schema(self) -> str:
           |        return "id INT"
           |
           |    def reader(self, schema):
           |        return SimpleDataSourceReader()
           |""".stripMargin
      val dataSource = createUserDefinedPythonDataSource(dataSourceName, dataSourceScript)
      spark.dataSource.registerPython(dataSourceName, dataSource)
      val err = intercept[AnalysisException](
        spark.read.format(dataSourceName).load().collect())
      assert(err.getErrorClass == "PYTHON_DATA_SOURCE_FAILED_TO_PLAN_IN_PYTHON")
      assert(err.getMessage.contains("PYTHON_DATA_SOURCE_CREATE_ERROR"))
    }
  }

  test("SPARK-46424: Support Python metrics") {
    assume(shouldTestPandasUDFs)
    val dataSourceScript =
      s"""
         |from pyspark.sql.datasource import DataSource, DataSourceReader
         |class SimpleDataSourceReader(DataSourceReader):
         |    def partitions(self):
         |        return []
         |
         |    def read(self, partition):
         |        if partition is None:
         |            yield ("success", )
         |        else:
         |            yield ("failed", )
         |
         |class $dataSourceName(DataSource):
         |    def schema(self) -> str:
         |        return "status STRING"
         |
         |    def reader(self, schema):
         |        return SimpleDataSourceReader()
         |""".stripMargin
    val dataSource = createUserDefinedPythonDataSource(dataSourceName, dataSourceScript)
    spark.dataSource.registerPython(dataSourceName, dataSource)
    val df = spark.read.format(dataSourceName).load()

    val statusStore = spark.sharedState.statusStore
    val oldCount = statusStore.executionsList().size

    df.collect()

    // Wait until the new execution is started and being tracked.
    while (statusStore.executionsCount() < oldCount) {
      Thread.sleep(100)
    }

    // Wait for listener to finish computing the metrics for the execution.
    while (statusStore.executionsList().isEmpty ||
      statusStore.executionsList().last.metricValues == null) {
      Thread.sleep(100)
    }

    val executedPlan = df.queryExecution.executedPlan.collectFirst {
      case p: BatchScanExec => p
    }
    assert(executedPlan.isDefined)

    val execId = statusStore.executionsList().last.executionId
    val metrics = statusStore.executionMetrics(execId)
    val pythonDataSent = executedPlan.get.metrics("pythonDataSent")
    val pythonDataReceived = executedPlan.get.metrics("pythonDataReceived")
    assert(metrics.contains(pythonDataSent.id))
    assert(metrics(pythonDataSent.id).asInstanceOf[String].endsWith("B"))
    assert(metrics.contains(pythonDataReceived.id))
    assert(metrics(pythonDataReceived.id).asInstanceOf[String].endsWith("B"))
  }
}
