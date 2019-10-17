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

package org.apache.zeppelin.spark


import java.io.File
import java.net.URLClassLoader
import java.nio.file.Paths
import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.LightningSQLEnv
import org.apache.spark.{JobProgressUtil, SparkConf, SparkContext}
import org.apache.zeppelin.interpreter.thrift.InterpreterCompletion
import org.apache.zeppelin.interpreter.util.InterpreterOutputStream
import org.apache.zeppelin.interpreter.{InterpreterContext, InterpreterResult}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._
import scala.tools.nsc.interpreter.Completion.ScalaCompleter
import scala.util.control.NonFatal

/**
  * Base class for different scala versions of SparkInterpreter. It should be
  * binary compatible between multiple scala versions.
  *
  * @param conf
  * @param depFiles
  */
abstract class LightningEnabledSparkScalaInterpreter(override val conf: SparkConf,
                                         override val depFiles: java.util.List[String],
                                         override val printReplOutput: java.lang.Boolean) extends BaseSparkScalaInterpreter(conf, depFiles, printReplOutput) {

  override protected def createSparkContext(): Unit = {
    LOGGER.info("Create Lightning Spark Context")
    spark2CreateLightningContext()
  }

  private def spark2CreateLightningContext(): Unit = {
    LOGGER.info("Lightning : spark2CreateLightningContext")

    val lightningConfFilePath = conf.get("com.zetaris.lightning.lightningConfFile")
    val metaStoreConfFile = conf.get("com.zetaris.lightning.metaStoreConfFile")

    if (lightningConfFilePath == null) {
      throw new RuntimeException("com.zetaris.lightning.lightningConfFile is not set in property");
    }

    if (lightningConfFilePath == null || metaStoreConfFile == null) {
      throw new RuntimeException("com.zetaris.lightning.metaStoreConfFile is not set in property");
    }

    try {
      sparkSession = new LightningSparkSessionBuilder().
        withConfiguration(lightningConfFilePath).
        withMetaStore(metaStoreConfFile).
        build()
      sc = LightningSQLEnv.sparkContext
      sqlContext = LightningSQLEnv.sqlContext
    } catch {
      case th: Throwable => throw new RuntimeException(th)
    }

    bind("spark", sparkSession.getClass.getCanonicalName, sparkSession, List("""@transient"""))
    bind("sc", "org.apache.spark.SparkContext", sc, List("""@transient"""))
    bind("sqlContext", "org.apache.spark.sql.SQLContext", sqlContext, List("""@transient"""))

    interpret("import org.apache.spark.SparkContext._")
    interpret("import spark.implicits._")
    interpret("import spark.sql")
    interpret("import org.apache.spark.sql.functions._")
  }

}
