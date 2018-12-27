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

import java.util.{Locale, Properties}

import com.amazonaws.auth.{AWSCredentials, AWSCredentialsProvider, InstanceProfileCredentialsProvider}
import com.amazonaws.regions.Regions

import scala.collection.JavaConverters._


class AthenaOptions(@transient val properties: Properties) extends AnyRef with Serializable {

  import AthenaOptions._

  private val options = properties.asScala

  require(options.get(ATHENA_S3_STAGING_DIR).isDefined, s"Option '$ATHENA_S3_STAGING_DIR' is required.")
  val region: String = if (options.get(ATHENA_REGION).isEmpty) Regions.getCurrentRegion.getName else options(ATHENA_REGION)
  val s3StagingDir: String = options(ATHENA_S3_STAGING_DIR)
  val credentials: AWSCredentialsProvider = if (options.get(ATHENA_USER).isDefined && options.get(ATHENA_PASSWORD).isDefined) {
    AthenaOptions.createCredentialsFromAccessKey(options(ATHENA_USER), options(ATHENA_PASSWORD))
  } else {
    InstanceProfileCredentialsProvider.getInstance()
  }
  val database: String = options.getOrElse(ATHENA_DATABASE, "default")
  val timeout: Int = options.getOrElse(ATHENA_TIMEOUT, "100000").toInt
  val maxRow: Int = options.getOrElse(ATHENA_MAX_ROW, "1000").toInt
  val maxBytes: Int = options.getOrElse(ATHENA_MAX_BYTES, "102400").toInt
  val sleepMs: Int = options.getOrElse(ATHENA_SLEEP_MS, "1000").toInt
  val s3ExpirationMs: Int = options.getOrElse(ATHENA_S3_EXPIRATION_HR, "72").toInt * 1000 * 60 * 60
  val concurrency: Int = options.getOrElse(ATHENA_CONCURRENCY, "10").toInt
}

object AthenaOptions extends AnyRef with Serializable {
  def apply(properties: Properties): AthenaOptions = new AthenaOptions(properties)

  def createCredentialsFromAccessKey(accessKeyId: String, secretKey: String): AWSCredentialsProvider =
    new AWSCredentialsProvider {
      override def getCredentials: AWSCredentials = new AWSCredentials {
        override def getAWSAccessKeyId: String = accessKeyId

        override def getAWSSecretKey: String = secretKey
      }

      override def refresh(): Unit = ()
    }


  private val athenaOptionNames = collection.mutable.Set.empty[String]

  private def newOption(name: String): String = {
    athenaOptionNames += name.toLowerCase(Locale.ROOT)
    name
  }

  val ATHENA_REGION: String = newOption(name = "athena.region")
  val ATHENA_S3_STAGING_DIR: String = newOption(name = "athena.s3.staging_dir")
  val ATHENA_S3_EXPIRATION_HR: String = newOption(name = "athena.s3.expiration_hr")
  val ATHENA_USER: String = newOption(name = "athena.user")
  val ATHENA_PASSWORD: String = newOption(name = "athena.password")
  val ATHENA_DATABASE: String = newOption(name = "athena.database")
  val ATHENA_TIMEOUT: String = newOption(name = "athena.timeout")
  val ATHENA_MAX_ROW: String = newOption(name = "athena.rows.max")
  val ATHENA_MAX_BYTES: String = newOption(name = "athena.bytes.max")
  val ATHENA_SLEEP_MS: String = newOption(name = "athena.sleep_ms")
  val ATHENA_CONCURRENCY: String = newOption(name = "athena.concurrency")
}
