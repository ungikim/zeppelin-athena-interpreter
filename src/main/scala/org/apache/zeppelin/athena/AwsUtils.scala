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

import com.amazonaws.ClientConfigurationFactory
import com.amazonaws.services.athena.model._
import com.amazonaws.services.athena.{AmazonAthena, AmazonAthenaClientBuilder}
import com.amazonaws.services.s3.model.AmazonS3Exception
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import org.apache.zeppelin.interpreter.InterpreterResult
import org.apache.zeppelin.interpreter.InterpreterResult.Code

import scala.collection.AbstractIterator

class AthenaExecutionIterator(@transient private val client: AmazonAthena,
                              private val executionId: ExecutionId) extends AbstractIterator[InterpreterResult] {
  private val executionRequest = new GetQueryExecutionRequest().withQueryExecutionId(executionId.executionId)
  private var isQueryStillRunning = true

  override def hasNext: Boolean = isQueryStillRunning

  @throws(classOf[AmazonAthenaException])
  override def next(): InterpreterResult = {
    val executionResult = client.getQueryExecution(executionRequest)
    try {
      QueryExecutionState.fromValue(executionResult.getQueryExecution.getStatus.getState) match {
        case state if state == QueryExecutionState.FAILED =>
          isQueryStillRunning = false
          val stateChangeReason = executionResult.getQueryExecution.getStatus.getStateChangeReason

          new InterpreterResult(Code.ERROR, s"Query Failed to run with Error Message: $stateChangeReason")
        case state if state == QueryExecutionState.CANCELLED =>
          isQueryStillRunning = false
          new InterpreterResult(Code.ERROR, s"Query was cancelled.")
        case state if state == QueryExecutionState.SUCCEEDED =>
          isQueryStillRunning = false
          new InterpreterResult(Code.SUCCESS)
        case _ =>
          new InterpreterResult(Code.INCOMPLETE)
      }
    } catch {
      case ex: IllegalArgumentException => new InterpreterResult(Code.ERROR, s"${ex.getMessage}")
    }
  }
}

class AthenaResultIterator(@transient private val client: AmazonAthena,
                           private val maxRows: Int,
                           private val executionId: ExecutionId) extends AbstractIterator[ResultSet] {
  private val resultsRequest = new GetQueryResultsRequest().withQueryExecutionId(executionId.executionId)
  private var isResultRemaining = true
  private var resultRows = 0

  override def hasNext: Boolean = isResultRemaining

  @throws(classOf[AmazonAthenaException])
  override def next(): ResultSet = {
    val results = client.getQueryResults(resultsRequest)
    resultRows += results.getResultSet.getRows.size()

    if (Option(results.getNextToken).nonEmpty) {
      resultsRequest.setNextToken(results.getNextToken)
    } else {
      isResultRemaining = false
    }

    if (resultRows >= maxRows) isResultRemaining = false

    results.getResultSet
  }
}

object AwsUtils {
  @throws(classOf[AmazonAthenaException])
  def setupAthenaClientConnection(options: AthenaOptions,
                                  userConfigurations: AthenaUserConfigurations): AmazonAthena = {
    AmazonAthenaClientBuilder.standard().withRegion(options.region).withClientConfiguration(new ClientConfigurationFactory().getConfig.withClientExecutionTimeout(options.timeout)).withCredentials(if (userConfigurations.authenticationInfo.isAnonymous) {
      options.credentials
    } else {
      AthenaOptions.createCredentialsFromAccessKey(userConfigurations.usernamePassword.get.getUsername, userConfigurations.usernamePassword.get.getPassword)
    }).build
  }

  @throws(classOf[AmazonS3Exception])
  def setupS3ClientConnection(options: AthenaOptions,
                              userConfigurations: AthenaUserConfigurations): AmazonS3 = {
    AmazonS3ClientBuilder.standard().withRegion(options.region).withCredentials(if (userConfigurations.authenticationInfo.isAnonymous) {
      options.credentials
    } else {
      AthenaOptions.createCredentialsFromAccessKey(userConfigurations.usernamePassword.get.getUsername, userConfigurations.usernamePassword.get.getPassword)
    }).build()
  }
}
