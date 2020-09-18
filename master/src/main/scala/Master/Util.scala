package Master

import java.io.File

import Master.Types.WorkerIndexType
import org.apache.logging.log4j.scala.Logging
import scalaj.http.Http
import scalapb.{GeneratedMessage, GeneratedMessageCompanion}

import scala.collection.parallel.CollectionConverters.RangeIsParallelizable
import scala.collection.parallel.immutable.ParSeq

class Util(dir: File, workerCount: Int, partitionCount: Int, partitionSize: Int) extends Logging {

  val masterDir = new File(dir, "master")

  def clean(): Unit = Common.Util.clean(masterDir)

  private val TIMEOUT_MS = 60 * 60 * 1000

  def send(workerIndex: WorkerIndexType, msg: GeneratedMessage): Object {
    def apply[ReturnType <: GeneratedMessage](returnType: GeneratedMessageCompanion[ReturnType]): ReturnType
  } = {
    val port = 65400 + workerIndex
    val response = Http(s"http://localhost:$port/")
      .postData(msg.toByteArray)
      .timeout(TIMEOUT_MS, TIMEOUT_MS)
      .asBytes
      .body
    new {
      def apply[ReturnType <: GeneratedMessage](returnType: GeneratedMessageCompanion[ReturnType]): ReturnType = returnType.parseFrom(response)
    }
  }

  def broadcast[T](f: WorkerIndexType => T): ParSeq[T] = (0 until workerCount).par.map(f)
}
