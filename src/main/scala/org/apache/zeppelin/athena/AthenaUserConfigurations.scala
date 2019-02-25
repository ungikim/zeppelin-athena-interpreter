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

import java.time.temporal.ChronoUnit
import java.time.{Instant, LocalDate, ZoneId}
import java.util.Date

import com.amazonaws.HttpMethod
import com.amazonaws.services.athena.AmazonAthena
import com.amazonaws.services.athena.model._
import com.amazonaws.services.s3.model.GeneratePresignedUrlRequest
import com.amazonaws.services.s3.{AmazonS3, AmazonS3URI}
import org.apache.zeppelin.interpreter.InterpreterResult.{Code, Type}
import org.apache.zeppelin.interpreter.{InterpreterContext, InterpreterResult}
import org.apache.zeppelin.user.{AuthenticationInfo, UsernamePassword}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._


case class ParagraphId(paragraphId: String)

case class ExecutionId(executionId: String)

@throws(classOf[AmazonAthenaException])
class AthenaUserConfigurations(@transient private val context: InterpreterContext,
                               @transient private val options: AthenaOptions) extends AnyRef with Serializable {
  private final val logger = LoggerFactory.getLogger(classOf[AthenaUserConfigurations])
  private final val paragraphIdExecutionIdMap = collection.concurrent.TrieMap.empty[ParagraphId, ExecutionId]
  private final val queryExecutionContext = new QueryExecutionContext().withDatabase(options.database)
  private final val resultConfiguration = new ResultConfiguration().withOutputLocation(options.s3StagingDir)
  private final val startQueryExecutionRequest = new StartQueryExecutionRequest().withQueryExecutionContext(queryExecutionContext).withResultConfiguration(resultConfiguration)
  final val authenticationInfo: AuthenticationInfo = context.getAuthenticationInfo
  final val usernamePassword: Option[UsernamePassword] = if (context.getAuthenticationInfo.isAnonymous) {
    None
  } else {
    Some(context.getAuthenticationInfo.getUserCredentials.getUsernamePassword(context.getReplName.split('.').headOption.getOrElse(context.getReplName)))
  }
  private final val athenaClient: AmazonAthena = AwsUtils.setupAthenaClientConnection(options, userConfigurations = this)
  private final val s3Client: AmazonS3 = AwsUtils.setupS3ClientConnection(options, userConfigurations = this)


  def executeSql(context: InterpreterContext, queryString: String, options: AthenaOptions, download: Boolean): InterpreterResult = {
    val paragraphId = ParagraphId(context.getParagraphId)
    logger.info(s"Paragraph Id: ${paragraphId.paragraphId}")

    if (paragraphIdExecutionIdMap.get(paragraphId).isEmpty) {
      val executionId = ExecutionId(athenaClient.startQueryExecution(startQueryExecutionRequest.withQueryString(queryString)).getQueryExecutionId)

      paragraphIdExecutionIdMap.put(paragraphId, executionId)

      val executionIterator = new AthenaExecutionIterator(athenaClient, executionId)
      while (executionIterator.hasNext) {
        executionIterator.next() match {
          case ir: InterpreterResult if ir.code() == Code.ERROR => {
            paragraphIdExecutionIdMap.remove(paragraphId)
            return ir
          }
          case ir: InterpreterResult if ir.code() != Code.SUCCESS => {
            if (paragraphIdExecutionIdMap.get(paragraphId).isDefined) {
              Thread.sleep(options.sleepMs)
            }
          }
          case _ => paragraphIdExecutionIdMap.remove(paragraphId)
        }
      }

      logger.info(s"Execution Id: ${executionId.executionId}")

      if (!download) {
        val resultIterator = new AthenaResultIterator(athenaClient, options.maxRow, executionId)
        val msg = new StringBuilder("%table ")
        while (resultIterator.hasNext) {
          val results = resultIterator.next()

          msg.append(printRows(results.getRows.asScala.toList))

          Thread.sleep(options.sleepMs)
        }
        return new InterpreterResult(Code.SUCCESS, msg.toString)
      }

      return new InterpreterResult(Code.SUCCESS, Type.HTML, s"<a href='${getPresignedUrl(executionId)}'>Download</a>")
    }

    new InterpreterResult(Code.KEEP_PREVIOUS_RESULT)
  }

  private def getPresignedUrl(executionId: ExecutionId): String = {
    val parsedUri = new AmazonS3URI(s"${options.s3StagingDir}${if (options.s3StagingDir.last != '/') '/' else ""}${executionId.executionId}.csv")

    val expiration = Calendar.getInstance().getTime	
    var expTimeMillis: Long = expiration.getTime	
    expTimeMillis += options.s3ExpirationMs	
    expiration.setTime(expTimeMillis)
    logger.info(s"Bucket Name: ${parsedUri.getBucket}, objectKey: ${parsedUri.getKey}")

    s3Client.generatePresignedUrl(new GeneratePresignedUrlRequest(parsedUri.getBucket, parsedUri.getKey).withMethod(HttpMethod.GET).withExpiration(expiration)).toString
  }

  private def printRows(rows: List[Row]): String = {
    val msg = new StringBuilder

    for (row <- rows) {
      msg.append(row.getData.asScala.map(datum => datum.getVarCharValue).mkString("\t"))
      msg.append("\n")
    }

    msg.toString
  }

  def cancelExecution(paragraphId: ParagraphId): Unit = {
    val executionId = paragraphIdExecutionIdMap.get(paragraphId)

    if (executionId.isDefined) {
      paragraphIdExecutionIdMap.remove(paragraphId)
      athenaClient.stopQueryExecution(new StopQueryExecutionRequest().withQueryExecutionId(executionId.toString))
    }
  }

  def shutdown(): Unit = {
    if (paragraphIdExecutionIdMap.nonEmpty) paragraphIdExecutionIdMap.clear()
    athenaClient.shutdown()
  }
}
