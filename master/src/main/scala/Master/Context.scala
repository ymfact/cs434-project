package Master

import java.io.File

import Common.RPCClient
import Common.Util.NamedParamForced._
import com.google.protobuf.ByteString
import org.apache.logging.log4j.scala.Logging
import protocall.ProtoCallGrpc.ProtoCallStub

import scala.collection.parallel.CollectionConverters.seqIsParallelizable
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContextExecutorService, Future}

class Context(x: NamedParam = Forced, rootDir: File, val workerCount: Int, partitionCount: Int, partitionSize: Int) extends Logging {

  private val util = new Util(rootDir = rootDir, workerCount = workerCount, partitionCount = partitionCount, partitionSize = partitionSize)
  private val workers = (0 until workerCount).map(workerIndex => RPCClient(workerIndex))

  implicit val ec: ExecutionContextExecutorService = util.ec

  def clean(): Unit = Common.Util.clean(util.masterDir)

  def broadcast[T](f: ProtoCallStub => Future[T]): Seq[T] = workers.par.map(worker => Await.result(f(worker), Duration.Inf)).seq.toSeq

  def processSample(data: Seq[ByteString]): Seq[ByteString] = util.processSample(data)
}
