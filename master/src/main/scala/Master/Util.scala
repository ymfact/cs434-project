package Master

import java.io.File

import Master.Types.WorkerIndexType
import com.google.protobuf.ByteString
import org.apache.logging.log4j.scala.Logging
import scalaj.http.Http
import scalapb.{GeneratedMessage, GeneratedMessageCompanion}

class Util(rootDir: File, workerCount: Int, partitionCount: Int, partitionSize: Int) extends Logging {

  val masterDir = new File(rootDir, "master")

  private val TIMEOUT_MS = 60 * 60 * 1000

  def send[ResultMsgType <: GeneratedMessage]
  (resultType: GeneratedMessageCompanion[ResultMsgType], workerIndex: WorkerIndexType, endpoint: String, msg: GeneratedMessage): ResultMsgType = {
    val port = 65400 + workerIndex
    val response = Http(s"http://localhost:$port/$endpoint")
      .postData(msg.toByteArray)
      .timeout(TIMEOUT_MS, TIMEOUT_MS)
      .asBytes
      .body
    resultType.parseFrom(response)
  }
  
  def processSample(data: ByteString) = {
    Common.Data.sort(data)
  }
}
