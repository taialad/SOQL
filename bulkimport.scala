package services.ci.salesforce

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshalling.Marshal
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import akka.util.ByteString
import cats.implicits._
import models.ci.salesforce.{JobId, BatchId}
import scala.concurrent.{ExecutionContext, Future}
import utils.SecureXml


case class SessionParams(host: String, sessionId: String)

case class BatchStatus(jobId: JobId, batchId: BatchId, state: String)

case class BatchResultId(value: String) extends AnyVal

case class BulkImport()(implicit system: ActorSystem, materializer: Materializer, executionContext: ExecutionContext) {
  val http = Http()

  def login(login: String, password: String): Future[SalesForceBulkError Either SessionParams] = {
    val bodyRequest =
      s"""<?xml version="1.0" encoding="utf-8" ?>
          |<env:Envelope xmlns:xsd="http://www.w3.org/2001/XMLSchema"
          |    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
          |    xmlns:env="http://schemas.xmlsoap.org/soap/envelope/">
          |  <env:Body>
          |    <n1:login xmlns:n1="urn:partner.soap.sforce.com">
          |      <n1:username>$login</n1:username>
          |      <n1:password>$password</n1:password>
          |    </n1:login>
          |  </env:Body>
          |</env:Envelope>""".stripMargin


    for {
      _entity <- Marshal(bodyRequest).to[RequestEntity]
      request = HttpRequest(
        method = HttpMethods.POST,
        uri = "https://login.salesforce.com/services/Soap/u/37.0",
        entity = _entity.withContentType(ContentTypes.`text/xml(UTF-8)`),
        headers = List[HttpHeader](RawHeader("SOAPAction", "login"))
      )
      response <- http.singleRequest(request)
      xmlResponse <- Unmarshal(response.entity).to[String]
    } yield {
      val root = SecureXml.loadString(xmlResponse)
      val sessionId = (root \\ "sessionId").text
      val serverUrl = (root \\ "serverUrl").text
      if (sessionId.isEmpty || serverUrl.isEmpty) {
        Either.left(LoginError)
      } else {
        val host = Uri.apply(serverUrl).authority.host.address()
        Either.right(SessionParams(host, sessionId))
      }
    }
  }

  def createJob(salesforceSessionParams: SessionParams, fromObj: String, chunkSize: Option[Int] = None): Future[SalesForceBulkError Either JobId] = {
    val bodyRequest =
      s"""<?xml version="1.0" encoding="UTF-8"?>
        |<jobInfo
        |    xmlns="http://www.force.com/2009/06/asyncapi/dataload">
        |  <operation>query</operation>
        |  <object>$fromObj</object>
        |  <concurrencyMode>Parallel</concurrencyMode>
        |  <contentType>CSV</contentType>
        |</jobInfo>""".stripMargin

    val chunkHeader = chunkSize.map(size => RawHeader("Sforce-Enable-PKChunking", "chunkSize=" + size.toString))

    for {
      _entity <- Marshal(bodyRequest).to[RequestEntity]

      request = HttpRequest(
        method = HttpMethods.POST,
        uri = s"https://${salesforceSessionParams.host}/services/async/33.0/job",
        entity = _entity.withContentType(ContentTypes.`text/xml(UTF-8)`),
        headers = List(RawHeader("X-SFDC-Session", salesforceSessionParams.sessionId)) ++ chunkHeader
      )
      response <- http.singleRequest(request)

      xmlResponse <- Unmarshal(response.entity).to[String]
    } yield {
      val root = SecureXml.loadString(xmlResponse)

      val id = (root \\ "id").text

      if (id.isEmpty) {
        Either.left(CreateJobError)
      } else {
        Either.right(JobId(id))
      }
    }
  }

  def closeJob(salesforceSessionParams: SessionParams, jobId: JobId): Future[SalesForceBulkError Either Unit] = {
    val bodyRequest =
      """<?xml version="1.0" encoding="UTF-8"?>
        |<jobInfo xmlns="http://www.force.com/2009/06/asyncapi/dataload">
        |  <state>Closed</state>
        |</jobInfo>""".stripMargin

    for {
      _entity <- Marshal(bodyRequest).to[RequestEntity]
      request = HttpRequest(
        method = HttpMethods.POST,
        uri = s"https://${salesforceSessionParams.host}/services/async/33.0/job/${jobId.value}",
        entity = _entity.withContentType(ContentTypes.`text/csv(UTF-8)`),
        headers = List[HttpHeader](RawHeader("X-SFDC-Session", salesforceSessionParams.sessionId))
      )
      response <- http.singleRequest(request)

      xmlResponse <- Unmarshal(response.entity).to[String]
    } yield {
      Either.right(())
    }
  }

  def launchBatch(salesforceSessionParams: SessionParams, jobId: JobId, query: String): Future[SalesForceBulkError Either BatchId] = {
    for {
      _entity <- Marshal(query).to[RequestEntity]

      request = HttpRequest(
        method = HttpMethods.POST,
        uri = s"https://${salesforceSessionParams.host}/services/async/33.0/job/${jobId.value}/batch",
        entity = _entity.withContentType(ContentTypes.`text/csv(UTF-8)`),
        headers = List[HttpHeader](RawHeader("X-SFDC-Session", salesforceSessionParams.sessionId))
      )
      response <- http.singleRequest(request)

      xmlResponse <- Unmarshal(response.entity).to[String]
    } yield {
      val root = SecureXml.loadString(xmlResponse)

      val id = (root \\ "id").text

      if (id.isEmpty) {
        Either.left(CreateBatchError)
      } else {
        Either.right(BatchId(id))
      }
    }
  }

  def batchStatus(salesforceSessionParams: SessionParams, jobId: JobId): Future[Seq[BatchStatus]] = {
    val request = HttpRequest(
      method = HttpMethods.GET,
      uri = s"https://${salesforceSessionParams.host}/services/async/33.0/job/${jobId.value}/batch",
      headers = List[HttpHeader](RawHeader("X-SFDC-Session", salesforceSessionParams.sessionId))
    )
    for {
      response <- http.singleRequest(request)

      xmlResponse <- Unmarshal(response.entity).to[String]
    } yield {
      val root = SecureXml.loadString(xmlResponse)

      (root \\ "batchInfo").map { batchInfoNode =>
        BatchStatus(
          jobId = JobId((batchInfoNode \ "jobId").text),
          batchId = BatchId((batchInfoNode \ "id").text),
          state = (batchInfoNode \ "state").text
        )
      }
    }
  }

  def batchResults(salesforceSessionParams: SessionParams, jobId: JobId, batchId: BatchId): Future[Seq[BatchResultId]] = {
    val request = HttpRequest(
      method = HttpMethods.GET,
      uri = s"https://${salesforceSessionParams.host}/services/async/33.0/job/${jobId.value}/batch/${batchId.value}/result",
      headers = List[HttpHeader](RawHeader("X-SFDC-Session", salesforceSessionParams.sessionId))
    )
    for {
      response <- http.singleRequest(request)
      xmlResponse <- Unmarshal(response.entity).to[String]
    } yield {
      val root = SecureXml.loadString(xmlResponse)

      (root \\ "result").map { resultNode =>
        BatchResultId(resultNode.text)
      }
    }
  }

  def result(salesforceSessionParams: SessionParams, jobId: JobId, batchId: BatchId, batchResultId: BatchResultId): Future[Source[ByteString, Any]] = {
    val request = HttpRequest(
    
