package Master

import java.io.File

import org.apache.logging.log4j.scala.Logging

import scala.collection.parallel.CollectionConverters.ImmutableIterableIsParallelizable
import scala.collection.parallel.ParIterable

class Context(dir:File, workerCount: Int, partitionCount: Int, partitionSize: Int) extends Logging {

  private val util = new Util(dir, workerCount, partitionCount, partitionSize)
  private val workers = (0 until workerCount).map(workerIndex => new Worker(util, workerIndex))

  def clean(): Unit = util.clean()

  def broadcast: ParIterable[Worker] = workers.par
}
