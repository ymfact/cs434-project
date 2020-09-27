package Master

import Common.Util.NamedParamForced._
import com.google.protobuf.ByteString
import org.apache.logging.log4j.scala.Logging
import protocall.ProtoCallGrpc.ProtoCallStub

import scala.collection.parallel.CollectionConverters.seqIsParallelizable
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

class Context(x: NamedParam = Forced, val workerDests: Seq[String]) extends Logging {

  private val util = new Util(workerDests = workerDests)

  def broadcast[T](f: ProtoCallStub => Future[T]): Seq[T] = util.workers.par.map(worker => Await.result(f(worker), Duration.Inf)).seq.toSeq

  def processSample(data: Seq[ByteString]): Seq[ByteString] = util.processSample(data)
}
