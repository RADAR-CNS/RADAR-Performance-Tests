package org.radarcns.performance

import java.io.{ByteArrayOutputStream, StringWriter}
import java.nio.charset.StandardCharsets
import java.util.Base64
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{ConcurrentHashMap, TimeUnit}

import com.fasterxml.jackson.core.JsonFactory
import com.typesafe.config.ConfigFactory
import io.gatling.core.scenario.Simulation
import io.gatling.core.Predef._
import io.gatling.core.body.StringBody
import io.gatling.http.Predef._
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericDatumWriter}
import org.apache.avro.io.EncoderFactory
import org.apache.trevni.avro.RandomData

import scala.util.Random
import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.concurrent.duration.Duration

class RadarPlatformTest extends Simulation {

  private val conf = ConfigFactory.parseResources("test.conf")
  private val baseURL = conf.getString("baseUrl")
  private val clientId = conf.getString("clientId")
  private val clientSecret = conf.getString("clientSecret")
  private val clientScopes = conf.getString("scopes")
  private val clientIdToPair = conf.getString("clientIdToPair")
  private val subjectLogin = conf.getString("subjectLogin")
  private val kafkaApiVersion = conf.getInt("kafkaApiVersion")
  private val projectName = conf.getString("projectName")
  private val sourceId = conf.getString("sourceId")
  private val clientIdToPairSecret = conf.getString("clientIdToPairSecret")
  private val keySchemaIds = new ConcurrentHashMap[String, Int]()
  private val valueSchemaIds = new ConcurrentHashMap[String, Int]()
  private val encoderFactory = new EncoderFactory()
  private val random = new Random()
  private val topics = csv("topics.csv")
  private val duration = conf.getDuration("duration", TimeUnit.SECONDS) seconds
  private val schemasRegistered = new AtomicBoolean()

  val phaseFeeder = new Feeder[Double] {
    override def hasNext = true

    override def next: Map[String, Double] = {
      Map("phase" -> random.nextDouble())
    }
  }

  private var refreshToken = ""

  val httpConf = http
    .baseURL(baseURL)
    .inferHtmlResources()
    .acceptHeader("*/*")
    .acceptEncodingHeader("gzip, deflate")
    .acceptLanguageHeader("fr,fr-fr;q=0.8,en-us;q=0.5,en;q=0.3")
    .connectionHeader("keep-alive")
    .userAgentHeader("Mozilla/5.0 (Macintosh; Intel Mac OS X 10.10; rv:33.0) Gecko/20100101 Firefox/33.0")

  val headers_http = Map(
    "Accept" -> """application/json"""
  )

  val authorization_header = "Basic " + Base64.getEncoder.encodeToString(clientId.concat(":").concat(clientSecret).getBytes(StandardCharsets.UTF_8))

  val authorization_header_for_client = "Basic " + Base64.getEncoder.encodeToString(clientIdToPair.concat(":").concat(clientIdToPairSecret).getBytes(StandardCharsets.UTF_8))

  //  val authorization_header = "Basic " + Base64.getEncoder.encodeToString("ManagementPortalapp:travel.COUNTRY.flowers".getBytes(StandardCharsets.UTF_8))

  val headers_http_authentication = Map(
    "Content-Type" -> """application/x-www-form-urlencoded""",
    "Accept" -> """application/json""",
    "Authorization" -> authorization_header
  )

  val headers_http_authentication_for_client = Map(
    "Content-Type" -> """application/x-www-form-urlencoded""",
    "Accept" -> """application/json""",
    "Authorization" -> authorization_header_for_client
  )
  val headers_http_authenticated = Map(
    "Accept" -> """application/json""",
    "Authorization" -> "Bearer ${access_token}"
  )

  val headers_http_authenticated_for_subject = Map(
    "Accept" -> """application/json""",
    "Authorization" -> "Bearer ${access_token_for_subject}"
  )

  private val backendCheck = scenario("GetSubjects")
    .exec(http("GetSchemas")
      .get("/schema/subjects").check(status.is(200)))
    .pause(1)
    .exec(http("GetTopics")
      .get("/kafka/topics").check(status.is(401)))
    .pause(1)
    .exec(http("Authentication")
      .post("/managementportal/oauth/token")
      .headers(headers_http_authentication)
      .formParam("grant_type", "password")
      .formParam("scope", clientScopes)
      .formParam("client_secret", clientSecret)
      .formParam("client_id", clientId)
      .formParam("username", "admin")
      .formParam("password", "admin")
      .check(jsonPath("$.access_token").saveAs("access_token"))).exitHereIfFailed
    .pause(1)
    .exec(http("Authenticated topics request")
      .get("/kafka/topics")
      .headers(headers_http_authenticated)
      .check(status.is(200)))
    .pause(1)
    .exec(http("PairApp")
      .get("/managementportal/api/oauth-clients/pair")
      .queryParam("clientId", clientIdToPair)
      .queryParam("login", subjectLogin)
      .headers(headers_http_authenticated)
      .check(status.is(200))
      .check(jsonPath("$.refreshToken").ofType[String].saveAs("refreshTokenForSubject"))
    )
       .pause(1)
    .exec(http("Get Token for App")
      .post("/managementportal/oauth/token")
      .queryParam("grant_type", "refresh_token")
      .queryParam("refresh_token", "${refreshTokenForSubject}")
      .headers(headers_http_authentication_for_client)
      .check(jsonPath("$.access_token").saveAs("access_token_for_subject"))).exitHereIfFailed
            .foreach(topics.records , "topic") {
              exec(flattenMapIntoAttributes("${topic}"))
                .exec(http("Check topic exists")
              .get("/kafka/topics/${topic}")
              .headers(headers_http_authenticated)
              .check(status.is(200))).exitHereIfFailed
                      .pause(1)
            .exec(http("GetKeySchemaId")
            .get("/schema/subjects/${topic}-key/versions/1")
            .check(jsonPath("$..id").ofType[Int].saveAs("keySchemaId")))
            .exec(session => {
              keySchemaIds.put(session("topic").as[String], session("keySchemaId").as[Int])
              session
            })
            //          .pause(1)
            .exec(http("GetValueSchemaId")
            .get("/schema/subjects/${topic}-value/versions/1")
            .check(jsonPath("$..id").ofType[Int].saveAs("valueSchemaId"))
          )
            .exec(session => {
              valueSchemaIds.put(session("topic").as[String], session("valueSchemaId").as[Int])
              //              println("value topic " , valueSchemaIds.get(session("topic").as[String]))
              session
            })
        }
////      pause(1)
          .feed(phaseFeeder)
//          .feed(topics.random)

          .doIf( session => keySchemaIds.containsKey(session("topic").as[String]) && initialDelay(session) < duration) {
//              pause(initialDelay(_))
//            .exec(http("Send Data ")
//              .post( "/kafka/topics/${topic}")
//              .header("Content-Type", s"application/vnd.kafka.avro.v${kafkaApiVersion}+json; charset=utf-8")
//              .headers(headers_http_authenticated_for_subject)
//              .body(StringBody(requestBody)))
            pause(initialDelay(_))
              .foreach(topics.records , "topic") {
                exec(flattenMapIntoAttributes("${topic}"))
                .repeat(session => ((duration - initialDelay(session)) / interval(session)).toInt) {
                pause(interval(_))
                  .exec(http("Send Data ")
                    .post("/kafka/topics/${topic}")
                    .header("Content-Type", s"application/vnd.kafka.avro.v${kafkaApiVersion}+json; charset=utf-8")
                    .headers(headers_http_authenticated_for_subject)
                    .body(StringBody(requestBody)))
              }
            }
        }
  setUp(
    backendCheck.inject(atOnceUsers(5))
  ).protocols(httpConf)


  private def requestBody(session: Session): String = {
    val valueClass = Class.forName(session("valueClass").as[String])
    val valueSchema = schema(valueClass)
    val recordCount = session("recordCount").as[String].toInt
    val records = new RandomData(valueSchema, recordCount)
    val writer = new StringWriter()
    val gen = new JsonFactory().createGenerator(writer)
    gen.writeStartObject()
    val topic = session("topic").as[String]
    gen.writeNumberField("key_schema_id", keySchemaIds.get(session("topic").as[String]))
    gen.writeNumberField("value_schema_id", valueSchemaIds.get(session("topic").as[String]))
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

      gen.writeRaw("{ \"key\": {"
        +"\"projectId\": {\"string\":\""+projectName+"\"},"
        +"\"userId\": \""+subjectLogin+"\","
        +"\"sourceId\": \""+sourceId+"\""
        +"}"
        +", \"value\": " + toJson(record) +  "}")
    }
    gen.writeEndArray()
    gen.writeEndObject()
    gen.flush()

    val  result = writer.toString
    result
  }

  private def schema(c: Class[_]): Schema = c.getDeclaredMethod("getClassSchema").invoke(null).asInstanceOf[Schema]

  private def initialDelay(session: Session): Duration = session("phase").as[Double] * interval(session)

  private def interval(session: Session): Duration = session("interval").as[String].toInt seconds

  private def toJson(record: GenericData.Record): String = {
    val writer = new GenericDatumWriter[GenericData.Record](record.getSchema)
    val out = new ByteArrayOutputStream()
    val encoder = encoderFactory.jsonEncoder(record.getSchema, out)
    writer.write(record, encoder)
    encoder.flush()
    new String(out.toByteArray, "UTF-8")
  }
}