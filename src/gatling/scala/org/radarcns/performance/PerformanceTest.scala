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
import org.radarcns.kafka.ObservationKey
//import org.radarcns.key.MeasurementKey

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.util.Random


class PerformanceTest  {
  private val encoderFactory = new EncoderFactory()
  private val random = new Random()
  private val conf = ConfigFactory.parseResources("test.conf")
  private val schemaRegistryUrl = conf.getString("schemaRegistry")
  private val restProxyUrl = conf.getString("restProxy")
  private val numberOfParticipants = conf.getInt("numberOfParticipants")
  private val duration = conf.getDuration("duration", TimeUnit.SECONDS) seconds
  private val kafkaApiVersion = conf.getInt("kafkaApiVersion")
  private var keySchemaId = 0
  private val schemaIds = new ConcurrentHashMap[String, Int]()

  private val topics = csv("topics.csv")
  private val schemasRegistered = new AtomicBoolean()
  val userIdFeeder = new Feeder[String] {
    override def hasNext = true

    override def next: Map[String, String] = Map("user" -> random.nextInt(numberOfParticipants).toString)
  }

  val phaseFeeder = new Feeder[Double] {
    override def hasNext = true

    override def next: Map[String, Double] = {
      Map("phase" -> random.nextDouble())
    }
  }

  private val performanceTest: ScenarioBuilder = scenario("PerformanceTest")
    .doIfOrElse(_ => schemasRegistered.compareAndSet(false, true)) {
//      exec(http("GetKeySchemaId")
//        .get(schemaRegistryUrl + "/subjects/key/versions/1")
//        .check(jsonPath("$..id").ofType[Int].saveAs("key")))
//        .exec(session => {
//          keySchemaId = session("key").as[Int]
//          session
//        })
        repeat(topics.records.size) {
          feed(topics.queue)
            .exec(http("GetTopics")
              .get(restProxyUrl + "/topics")
              .check(jsonPath("$[?(@=='${topic}')]").count.transform(_ > 0).saveAs("verify_topic")))
            .doIf(_ ("verify_topic").as[Boolean]) {
              exec(http("GetValueSchemaId")
                .get(schemaRegistryUrl + "/subjects/${topic}-value/versions/1")
                .check(jsonPath("$..id").ofType[Int].saveAs("schemaId"))
              ).exec(session => {
                schemaIds.put(session("topic").as[String], session("schemaId").as[Int])
                session
              })
            }
        }
    } {
      feed(userIdFeeder)
        .feed(phaseFeeder)
        .feed(topics.random)
        .doIf(session => schemaIds.containsKey(session("topic").as[String]) && initialDelay(session) < duration) {
          pause(initialDelay(_))
            .exec(http("Performance Test")
              .post(restProxyUrl + "/topics/${topic}")
              .header("Content-Type", s"application/vnd.kafka.avro.v${kafkaApiVersion}+json; charset=utf-8")
              .header("X-User-ID", _ ("user").as[String])
              .body(StringBody(requestBody)))
            .repeat(session => ((duration - initialDelay(session)) / interval(session)).toInt) {
              pause(interval(_))
                .exec(http("Performance Test")
                  .post(restProxyUrl + "/topics/${topic}")
                  .header("Content-Type", s"application/vnd.kafka.avro.v${kafkaApiVersion}+json; charset=utf-8")
                  .header("X-User-ID", _ ("user").as[String])
                  .body(StringBody(requestBody)))
            }
        }
    }

//  setUp(
//    performanceTest.inject(
//      atOnceUsers(1),
//      nothingFor(2 seconds),
//      atOnceUsers(numberOfParticipants * topics.records.size)
//    )
//  ).protocols(http)

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

      gen.writeRaw("{\"key\": " + new ObservationKey(null,session("user").as[String], "SOURCE") + ", \"value\": " + toJson(record) + "}")
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

  private def initialDelay(session: Session): Duration = session("phase").as[Double] * interval(session)

  private def interval(session: Session): Duration = session("interval").as[String].toInt seconds
}