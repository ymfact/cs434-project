package Master

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{ConcurrentLinkedQueue, Executors}

import Common.Util.{getMyAddress, unitToEmpty}
import org.apache.logging.log4j.scala.Logging
import protocall.MasterServiceGrpc.MasterService
import protocall.{DataForConnect, Empty}

import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService, Future}
import scala.jdk.CollectionConverters.CollectionHasAsScala

class WorkerListener(workerCount: Int) extends Logging {
  private implicit val ec: ExecutionContextExecutorService = ExecutionContext.fromExecutorService(Executors.newSingleThreadExecutor)

  val workerDests = new ConcurrentLinkedQueue[String]
  private val server = Common.Server.startMaster(new MasterServiceImpl, logger)

  def thisDest: String = s"$getMyAddress:${server.getPort}"

  class MasterServiceImpl extends MasterService {
    override def connect(request: DataForConnect): Future[Empty] = {
      workerDests.add(request.dest)
      logger.info(s"connected: ${request.dest}")
      if (workerDests.size() == workerCount) {
        logger.info(s"All workers are attached.")
        Future {
          if (server != null)
            server.shutdown()
        }
      }
      Future.successful()
    }
  }

  def blockUntilShutdown(): Unit =
    if(server != null)
      server.awaitTermination()
}
