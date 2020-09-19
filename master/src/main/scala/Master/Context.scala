package Master

import java.io.File

import com.google.protobuf.ByteString
import org.apache.logging.log4j.scala.Logging

import scala.collection.parallel.CollectionConverters.ImmutableIterableIsParallelizable
import scala.collection.parallel.{ParIterable, ParSeq}

class Context(rootDir:File, workerCount: Int, partitionCount: Int, partitionSize: Int) extends Logging {

  private val util = new Util(rootDir, workerCount, partitionCount, partitionSize)
  private val workers = (0 until workerCount).map(workerIndex => new Worker(util, workerIndex))

  def clean(): Unit = Common.Util.clean(util.masterDir)

  def broadcast: ParIterable[Worker] = workers.par

  def processSample(data: ParSeq[ByteString]) = util.processSample(data)
}
