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

import java.util.Properties

import org.apache.zeppelin.interpreter.Interpreter.FormType
import org.apache.zeppelin.interpreter.InterpreterResult.Code
import org.apache.zeppelin.interpreter._

class AthenaDownloadInterpreter(properties: Properties) extends Interpreter(properties) {
  private def getAthenaInterpreter(): Option[AthenaInterpreter] = getAthenaInterpreter(getInterpreterInTheSameSessionByClassName(classOf[AthenaInterpreter].getName))

  private def getAthenaInterpreter(interpreter: Interpreter): Option[AthenaInterpreter] = interpreter match {
    case a: AthenaInterpreter => Some[AthenaInterpreter](a)
    case lzy: LazyOpenInterpreter =>
      val p = lzy.getInnerInterpreter
      lzy.open()
      getAthenaInterpreter(p)
    case w: WrappedInterpreter => getAthenaInterpreter(w.getInnerInterpreter)
    case _ => None
  }

  override def open(): Unit = if (getAthenaInterpreter().isDefined) getAthenaInterpreter().get.open()

  override def close(): Unit = if (getAthenaInterpreter().isDefined) getAthenaInterpreter().get.close()

  override def interpret(cmd: String, context: InterpreterContext): InterpreterResult = {
    val interpreter = getAthenaInterpreter()
    if (interpreter.isDefined) {
      interpreter.get.executeSql(cmd, context, download = true)
    } else {
      new InterpreterResult(Code.ERROR, "Cannot find Athena Interpreter")
    }
  }

  override def cancel(context: InterpreterContext): Unit = if (getAthenaInterpreter().isDefined) getAthenaInterpreter().get.cancel(context)

  override def getFormType: Interpreter.FormType = FormType.NONE

  override def getProgress(context: InterpreterContext): Int = 0
}
