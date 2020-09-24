package Master

import java.io.File

import Common.RPCClient
import Common.Util.NamedParamForced._
import com.google.protobuf.ByteString
import org.apache.logging.log4j.scala.Logging
import protocall.MasterServiceGrpc.MasterService
import protocall.ProtoCallGrpc.ProtoCallStub

import scala.collection.parallel.CollectionConverters.seqIsParallelizable
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContextExecutorService, Future}

class Context(x: NamedParam = Forced, val workerCount: Int) extends Logging {

  private val util = new Util(workerCount = workerCount)
  private var workers: Seq[ProtoCallStub] = Seq()

  def ec: ExecutionContextExecutorService = util.ec

  def thisDest(): String = util.thisDest()

  def startServer(masterService: MasterService): Unit = util.startServer(masterService)

  def stopServer(): Unit = util.stopServer()

  def blockUntilShutdown(): Unit = util.blockUntilShutdown()

  def connect(dests: Seq[String]): Unit = {
    workers = dests.map(RPCClient.worker)
  }

  def broadcast[T](f: ProtoCallStub => Future[T]): Seq[T] = workers.par.map(worker => Await.result(f(worker), Duration.Inf)).seq.toSeq

  def processSample(data: Seq[ByteString]): Seq[ByteString] = util.processSample(data)
}
