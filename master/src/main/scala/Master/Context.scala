package Master

import Common.Util.NamedParamForced._
import com.google.protobuf.ByteString
import org.apache.logging.log4j.scala.Logging
import protocall.ProtoCallGrpc.ProtoCallStub

import scala.collection.parallel.CollectionConverters.seqIsParallelizable
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

class Context(x: NamedParam = Forced, val workerCount: Int) extends Logging {

  private val util = new Util(workerCount = workerCount)

  def thisDest(): String = util.thisDest()

  def awaitWorkers(andThen: => Unit): Unit = util.awaitWorkers(andThen)

  def workerDests: Seq[String] = util.workerDests

  def broadcast[T](f: ProtoCallStub => Future[T]): Seq[T] = util.workers.par.map(worker => Await.result(f(worker), Duration.Inf)).seq.toSeq

  def processSample(data: Seq[ByteString]): Seq[ByteString] = util.processSample(data)
}
