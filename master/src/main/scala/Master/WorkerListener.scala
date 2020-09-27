package Master

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{ConcurrentLinkedQueue, Executors}

import Common.Util.{getMyAddress, unitToEmpty}
import org.apache.logging.log4j.scala.Logging
import protocall.MasterServiceGrpc.MasterService
import protocall.{DataForConnect, Empty}

import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService, Future}
import scala.jdk.CollectionConverters.CollectionHasAsScala

class WorkerListener(workerCount: Int, andThen: Seq[String] => Unit) extends Logging {
  private implicit val ec: ExecutionContextExecutorService = ExecutionContext.fromExecutorService(Executors.newSingleThreadExecutor)

  private val workerDests = new ConcurrentLinkedQueue[String]
  private val isRunStarted = new AtomicBoolean
  private val server = Common.Server.startMaster(new MasterServiceImpl, logger)

  def thisDest: String = s"$getMyAddress:${server.getPort}"

  class MasterServiceImpl extends MasterService {
    override def connect(request: DataForConnect): Future[Empty] = {
      workerDests.add(request.dest)
      logger.info(s"connected: ${request.dest}")
      if (workerDests.size() == workerCount)
        if (!isRunStarted.getAndSet(true)) {
          logger.info(s"All workers are attached.")
          if (server != null)
            server.shutdown()
          Future {
            andThen(workerDests.asScala.toSeq)
          }
        }
      Future.successful()
    }
  }

}

object WorkerListener{
  def listenAndGetWorkerDests(workerCount: Int)(andThen: Seq[String] => Unit): WorkerListener =
    new WorkerListener(workerCount, andThen)
}