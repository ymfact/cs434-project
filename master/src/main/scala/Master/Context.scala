package Master

import java.io.File

import Master.Types.WorkerIndexType
import org.apache.logging.log4j.scala.Logging
import scalapb.{GeneratedMessage, GeneratedMessageCompanion}

import scala.collection.parallel.immutable.ParSeq

class Context(dir:File, workerCount: Int, partitionCount: Int, partitionSize: Int) extends Logging {

  private val util = new Util(dir, workerCount, partitionCount, partitionSize)

  def workerDir: File = util.masterDir

  def clean(): Unit = util.clean()

  def send: (WorkerIndexType, GeneratedMessage) => Object {
    def apply[ReturnType <: GeneratedMessage](returnType: GeneratedMessageCompanion[ReturnType]): ReturnType
  } = util.send

  def broadcast[T]: (WorkerIndexType => T) => ParSeq[T] = util.broadcast[T]
}
