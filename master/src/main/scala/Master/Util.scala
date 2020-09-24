package Master

import java.io.DataInputStream
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{ConcurrentLinkedQueue, Executors}

import Common.Const.BYTE_COUNT_IN_RECORD
import Common.Util.NamedParamForced._
import Common.Util.{getMyAddress, unitToEmpty}
import Common.{RPCClient, RecordStream, Sorts}
import com.google.protobuf.ByteString
import io.grpc.Server
import org.apache.logging.log4j.scala.Logging
import protocall.MasterServiceGrpc.MasterService
import protocall.ProtoCallGrpc.ProtoCallStub
import protocall.{DataForConnect, Empty}

import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService, Future}
import scala.jdk.CollectionConverters.CollectionHasAsScala

class Util(x: NamedParam = Forced, workerCount: Int) extends Logging {

  implicit val ec: ExecutionContextExecutorService = ExecutionContext.fromExecutorService(Executors.newSingleThreadExecutor)

  var workers: Seq[ProtoCallStub] = Seq()
  var workerDests: Seq[String] = Seq()

  private var server: Server = _

  def thisDest(): String = s"$getMyAddress:${server.getPort}"

  def awaitWorkers(andThen: => Unit): Unit = {
    val workerDests: ConcurrentLinkedQueue[String] = new ConcurrentLinkedQueue
    val isRunStarted: AtomicBoolean = new AtomicBoolean

    server = Common.Server.startMaster(new MasterServiceImpl, logger)

    class MasterServiceImpl extends MasterService {
      override def connect(request: DataForConnect): Future[Empty] = {
        workerDests.add(request.dest)
        logger.info(s"connected: ${request.dest}")
        if(workerDests.size() == workerCount)
          if(!isRunStarted.getAndSet(true)) {
            logger.info(s"All workers are attached.")
            Util.this.workerDests = workerDests.asScala.toSeq
            workers = Util.this.workerDests.map(RPCClient.worker)
            if(server != null)
              server.shutdown()
            Future{
              andThen
            }
          }
        Future.successful()
      }
    }
  }

  def processSample(data: Seq[ByteString]): Seq[ByteString] = {
    val recordArrays = data.map(_.newInput).map(new DataInputStream(_)).map(RecordStream.from)
    val sorted = Sorts.sortFromSorteds(recordArrays)
    sorted.grouped(data.head.size() / BYTE_COUNT_IN_RECORD).map(_.head.copyKey).drop(1).toSeq
  }
}
