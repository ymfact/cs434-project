package Common

import java.io.File
import java.net.ConnectException

import com.google.protobuf.empty.Empty
import org.apache.logging.log4j.scala.Logging
import io.undertow.util.FileUtils
import scalapb.{GeneratedMessage, GeneratedMessageCompanion}
import scalaj.http.Http

import scala.annotation.tailrec

object Util extends Logging {
  def clean(dir: File){
    logger.info("clean")
    FileUtils.deleteRecursive(dir.toPath)
  }

  implicit def unitToEmpty (unit: Unit) = new Empty

  def send[OrderMsgType <: GeneratedMessage, ResultMsgType <: GeneratedMessage]
  (workerIndex: Int, protocol: Protocol[OrderMsgType, ResultMsgType], msg: GeneratedMessage = new Empty): ResultMsgType =
    Common.Util.send(protocol.ResultType, workerIndex, protocol.endpoint, msg)

  private val TIMEOUT_MS = 60 * 60 * 1000

  private def send[ResultMsgType <: GeneratedMessage]
  (resultType: GeneratedMessageCompanion[ResultMsgType], workerIndex: Int, endpoint: String, msg: GeneratedMessage): ResultMsgType = {
    val port = 65400 + workerIndex
    val response = Http(s"http://localhost:$port/$endpoint")
      .timeout(TIMEOUT_MS, TIMEOUT_MS)
      .postData(msg.toByteArray)
      .asBytes
      .body
    resultType.parseFrom(response)
  }
}
