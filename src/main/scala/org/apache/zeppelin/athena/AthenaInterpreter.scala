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

package org.apache.zeppelin.athena

import java.util
import java.util.Properties

import org.apache.zeppelin.interpreter.Interpreter.FormType
import org.apache.zeppelin.interpreter.InterpreterResult.Code
import org.apache.zeppelin.interpreter.thrift.InterpreterCompletion
import org.apache.zeppelin.interpreter.{Interpreter, InterpreterContext, InterpreterResult}
import org.apache.zeppelin.scheduler.{Scheduler, SchedulerFactory}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

case class User(user: String)

class AthenaInterpreter(properties: Properties) extends Interpreter(properties) {
  private final val logger = LoggerFactory.getLogger(classOf[AthenaInterpreter])
  private final val options = AthenaOptions(getProperties)
  private final lazy val ruleManager = new AthenaRuleManager(options)
  private final val userConfigurations = collection.mutable.Map.empty[User, AthenaUserConfigurations]

  override def open(): Unit = ()

  override def getScheduler: Scheduler = SchedulerFactory.singleton.createOrGetParallelScheduler(s"${classOf[AthenaInterpreter].getName}$hashCode", options.concurrency)

  override def close(): Unit = {
    if (userConfigurations.nonEmpty) {
      userConfigurations.foreach(elem => elem._2.shutdown())
      userConfigurations.clear()
    }
  }

  override def interpret(cmd: String, context: InterpreterContext): InterpreterResult = executeSql(cmd, context, download = false)

  def executeSql(cmd: String, context: InterpreterContext, download: Boolean): InterpreterResult = {
    logger.info(s"ReplName: ${context.getReplName}")
    if (cmd == null || cmd.trim.isEmpty) new InterpreterResult(Code.ERROR, "No query")

    val athenaCheckResult = ruleManager.assertAthenaLimit(cmd)
    logger.info(s"Assert Athena Limit Result: ${athenaCheckResult.toJson}")
    if (athenaCheckResult.code() != Code.SUCCESS) return athenaCheckResult

    val userConfigurations = getAthenaUserConfigurations(context)

    var query = cmd
    if (!download) {
      val limitCheckResult = ruleManager.assertLimitClause(cmd)
      logger.info(s"Assert Limit Result: ${limitCheckResult.message}")
      limitCheckResult match {
        case LimitRules.NoLimit => query = s"$cmd LIMIT ${options.maxRow}"
        case LimitRules.LimitExceeds => return new InterpreterResult(Code.ERROR, s"Limit clause exceeds ${options.maxRow}")
        case _ =>
      }
    }

    userConfigurations.executeSql(context, query, options, download)
  }

  private def getAthenaUserConfigurations(context: InterpreterContext): AthenaUserConfigurations = {
    val user = User(context.getAuthenticationInfo.getUser)

    if (userConfigurations.get(user).isEmpty) {
      userConfigurations(user) = new AthenaUserConfigurations(context, options)
    }

    userConfigurations(user)
  }

  override def cancel(context: InterpreterContext): Unit = {
    val paragraphId = ParagraphId(context.getParagraphId)

    getAthenaUserConfigurations(context).cancelExecution(paragraphId)
  }

  override def getFormType: Interpreter.FormType = FormType.SIMPLE

  override def completion(buf: String, cursor: Int, context: InterpreterContext): util.List[InterpreterCompletion] = List.empty[InterpreterCompletion].asJava

  override def getProgress(context: InterpreterContext): Int = 0
}
