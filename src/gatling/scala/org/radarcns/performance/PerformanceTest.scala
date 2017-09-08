/*
 * Copyright 2017 King's College London and The Hyve
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.radarcns.performance

import java.io.{ByteArrayOutputStream, StringWriter}
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{ConcurrentHashMap, TimeUnit}

import com.fasterxml.jackson.core.JsonFactory
import com.typesafe.config.ConfigFactory
import io.gatling.core.Predef._
import io.gatling.core.body.StringBody
import io.gatling.core.structure.ScenarioBuilder
import io.gatling.http.Predef._
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericDatumWriter}
import org.apache.avro.io.EncoderFactory
import org.apache.trevni.avro.RandomData
import org.radarcns.key.MeasurementKey

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.util.Random


class PerformanceTest extends Simulation {
  private val encoderFactory = new EncoderFactory()
  private val random = new Random()
  private val conf = ConfigFactory.parseResources("test.conf")
  private val schemaRegistryUrl = conf.getString("schemaRegistry")
  private val restProxyUrl = conf.getString("restProxy")
  private val numberOfParticipants = conf.getInt("numberOfParticipants")
  private val durationMs = conf.getDuration("duration", TimeUnit.MILLISECONDS)
  private val userId = if (conf.hasPath("userId")) conf.getString("userId") else ""

  private var keySchemaId = 0
  private val schemaIds = new ConcurrentHashMap[String, Int]()

  private val topics = csv("topics.csv")
  private val schemasRegistered = new AtomicBoolean()

  private val performanceTest: ScenarioBuilder = scenario("PerformanceTest")
    .doIf(_ => schemasRegistered.compareAndSet(false, true)) {
      exec(http("RegisterKeySchema")
        .post(schemaRegistryUrl + "/subjects/key/versions")
        .header("Content-Type", "application/vnd.schemaregistry.v1+json")
        .body(StringBody(schemaWrapper(classOf[MeasurementKey])))
        .check(jsonPath("$..id").ofType[Int].saveAs("key"))
      )
        .exec(session => {
          keySchemaId = session("key").as[Int]
          session
        })
        .repeat(topics.records.size) {
          feed(topics.queue)
            .exec(http("RegisterValueSchemas")
              .post(schemaRegistryUrl + "/subjects/${topic}-value/versions")
              .header("Content-Type", "application/vnd.schemaregistry.v1+json")
              .body(StringBody(valueSchema))
              .check(jsonPath("$..id").ofType[Int].saveAs("schemaId"))
            )
            .exec(session => {
              schemaIds.put(session("topic").as[String], session("schemaId").as[Int])
              session
            })
        }
    }
    .feed(topics.random)
    .pause(session => random.nextInt((session("frequency").as[String].toDouble * 1000).toInt) milliseconds)
    .during(durationMs milliseconds) {
      pace(session => session("frequency").as[String].toDouble * 1000 milliseconds)
        .exec(http("Performance Test")
          .post(restProxyUrl + "/topics/${topic}")
          .header("Content-Type", "application/vnd.kafka.avro.v2+json; charset=utf-8")
          .header("X-User-ID", userId)
          .body(StringBody(requestBody)))
    }

  setUp(
    performanceTest.inject(
      atOnceUsers(1),
      nothingFor(10 seconds),
      atOnceUsers(numberOfParticipants * topics.records.size)
    )
  ).protocols(http)

  private def valueSchema(session: Session): String = {
    val valueClass = Class.forName(session("valueClass").as[String])
    schemaWrapper(valueClass)
  }

  private def schemaWrapper(valueClass: Class[_]): String = {
    val writer = new StringWriter()
    val gen = new JsonFactory().createGenerator(writer)
    gen.writeStartObject()
    gen.writeStringField("schema", schema(valueClass).toString)
    gen.writeEndObject()
    gen.flush()
    writer.toString
  }

  private def requestBody(session: Session): String = {
    val valueClass = Class.forName(session("valueClass").as[String])
    val valueSchema = schema(valueClass)
    val recordCount = session("recordCount").as[String].toInt
    val records = new RandomData(valueSchema, recordCount)
    val writer = new StringWriter()
    val gen = new JsonFactory().createGenerator(writer)
    gen.writeStartObject()
    val topic = session("topic").as[String]
    gen.writeNumberField("key_schema_id", keySchemaId)
    gen.writeNumberField("value_schema_id", schemaIds.get(topic))
    gen.writeArrayFieldStart("records")
    var first = true
    for (record <- records.asScala.map(_.asInstanceOf[GenericData.Record])) {
      val now = System.currentTimeMillis() / 1000.0
      record.put("time", now)
      record.put("timeReceived", now)
      if (!first) {
        gen.writeRaw(", ")
      }
      first = false

      val user = if (userId.isEmpty) "USR" + session.userId else userId
      gen.writeRaw("{\"key\": " + new MeasurementKey(user, "SOURCE") + ", \"value\": " + toJson(record) + "}")
    }
    gen.writeEndArray()
    gen.writeEndObject()
    gen.flush()

    writer.toString
  }

  private def toJson(record: GenericData.Record): String = {
    val writer = new GenericDatumWriter[GenericData.Record](record.getSchema)
    val out = new ByteArrayOutputStream()
    val encoder = encoderFactory.jsonEncoder(record.getSchema, out)
    writer.write(record, encoder)
    encoder.flush()
    new String(out.toByteArray, "UTF-8")
  }

  private def schema(c: Class[_]): Schema = c.getDeclaredMethod("getClassSchema").invoke(null).asInstanceOf[Schema]
}