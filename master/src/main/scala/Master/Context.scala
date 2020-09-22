package Master

import java.io.File

import Common.Util.NamedParamForced._
import com.google.protobuf.ByteString
import org.apache.logging.log4j.scala.Logging

import scala.collection.parallel.CollectionConverters.ImmutableIterableIsParallelizable
import scala.collection.parallel.ParIterable

class Context(x: NamedParam = Forced, rootDir: File, workerCount: Int, partitionCount: Int, partitionSize: Int) extends Logging {

  private val util = new Util(rootDir = rootDir, workerCount = workerCount, partitionCount = partitionCount, partitionSize = partitionSize)
  private val workers = (0 until workerCount).map(workerIndex => new Worker(workerIndex))

  def clean(): Unit = Common.Util.clean(util.masterDir)

  def broadcast: ParIterable[Worker] = workers.par

  def processSample(data: Seq[ByteString]): Seq[ByteString] = util.processSample(data)
}
