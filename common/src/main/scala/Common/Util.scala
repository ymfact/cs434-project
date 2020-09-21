package Common

import java.io.File

import com.google.protobuf.empty.Empty
import io.undertow.util.FileUtils
import org.apache.logging.log4j.scala.Logging
import scalaj.http.Http
import scalapb.{GeneratedMessage, GeneratedMessageCompanion}

object Util extends Logging {

  private val TIMEOUT_MS = 60 * 1000

  def clean(dir: File) {
    logger.info("clean")
    FileUtils.deleteRecursive(dir.toPath)
  }

  implicit def unitToEmpty(unit: Unit) = new Empty

  def cleanTemp(dir: File): Unit = {
    logger.info(s"clean temp*")
    dir.listFiles.filter(_.getName.startsWith("temp")).foreach(_.delete)
  }

  def send[OrderMsgType <: GeneratedMessage, ResultMsgType <: GeneratedMessage]
  (workerIndex: Int, protocol: Protocol[OrderMsgType, ResultMsgType], msg: GeneratedMessage = new Empty): ResultMsgType =
    Common.Util.send(protocol.ResultType, workerIndex, protocol.endpoint, msg)

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

  def log2(x: Double): Double = math.log(x) / math.log(2)

  def byteToUnsigned(byte: Byte): Int = byte & 0xff

  object NamedParamForced {

    val Forced = new NamedParam(42)

    class NamedParam(val i: Int) extends AnyVal
  }

}