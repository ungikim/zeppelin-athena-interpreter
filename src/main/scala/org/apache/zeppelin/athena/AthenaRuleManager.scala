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

import java.util.Locale

import org.apache.zeppelin.interpreter.InterpreterResult
import org.apache.zeppelin.interpreter.InterpreterResult.{Code, Type}

object LimitRules {

  sealed abstract class Rule(val message: String) {
    override def toString: String = message
  }

  case object Ok extends Rule("Ok")

  case object NoLimit extends Rule("No limit clause")

  case object LimitExceeds extends Rule("Limit clause exceeds")

}

class AthenaRuleManager(private val options: AthenaOptions) {
  private final val athenaLimitationsResult = new InterpreterResult(Code.ERROR, Type.HTML, s"See also <a href='https://docs.aws.amazon.com/athena/latest/ug/other-notable-limitations.html'>Limitations</a>")

  def assertAthenaLimit(cmd: String): InterpreterResult = cmd.trim.toLowerCase(Locale.ROOT) match {
    case parsedSql if parsedSql.startsWith("explain") => athenaLimitationsResult
    case parsedSql if parsedSql.startsWith("create table like") => athenaLimitationsResult
    case parsedSql if parsedSql.startsWith("describe input") => athenaLimitationsResult
    case parsedSql if parsedSql.startsWith("describe output") => athenaLimitationsResult
    case _ => new InterpreterResult(Code.SUCCESS)
  }

  def assertLimitClause(cmd: String): LimitRules.Rule = cmd.trim.toLowerCase(Locale.ROOT) match {
    case parsedSql if parsedSql.startsWith("select") =>
      val tokens = parsedSql.replace('\n', ' ')
        .replace('\r', ' ')
        .replace('\t', ' ')
        .replace("(\\s*)", " ")
        .split(" ").filter(_.nonEmpty)

      if (tokens.length < 2) LimitRules.NoLimit

      if (tokens(tokens.length - 2).trim.equals("limit")) {
        val limit = tokens(tokens.length - 1).trim.toInt
        if (limit > options.maxRow) {
          LimitRules.LimitExceeds
        } else {
          LimitRules.Ok
        }
      } else {
        LimitRules.NoLimit
      }
    case _ => LimitRules.Ok
  }
}
