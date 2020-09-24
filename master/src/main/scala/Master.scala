import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicBoolean

import Common.Util.unitToEmpty
import Master.Context
import com.google.protobuf.ByteString
import org.apache.logging.log4j.scala.Logging
import protocall.MasterServiceGrpc.MasterService
import protocall.{DataForClassify, DataForConnect, Empty}

import scala.concurrent.{ExecutionContextExecutorService, Future}
import scala.jdk.CollectionConverters.CollectionHasAsScala

class Master(ctx: Context) extends Logging {

  implicit val ec: ExecutionContextExecutorService = ctx.ec
  private val workerDests: ConcurrentLinkedQueue[String] = new ConcurrentLinkedQueue
  private val isRunStarted: AtomicBoolean = new AtomicBoolean

  ctx.startServer(new MasterServiceImpl)
  private class MasterServiceImpl extends MasterService {
    override def connect(request: DataForConnect): Future[Empty] = {
      workerDests.add(request.dest)
      logger.info(s"connected: ${request.dest}")
      if(workerDests.size() == ctx.workerCount)
        if(!isRunStarted.getAndSet(true)) {
          logger.info(s"All workers are attached.")
          ctx.stopServer()
          Future {
            ctx.connect(workerDests.asScala.toSeq)
            run()
          }
        }
      Future.successful()
    }
  }

  def thisDest(): String = ctx.thisDest()

  def blockUntilShutdown(): Unit = ctx.blockUntilShutdown()

  private def run(): Unit = {

    val samples: Seq[ByteString] = ctx.broadcast {
      logger.info(s"Sending sample request...")
      _.sample()
    }.map( result =>{
      logger.info(s"Received sample result.")
      result.bytes
    })

    samples.foreach(sample => logger.info(s"sample received: ${sample.size}"))

    val sampleRanges: Seq[ByteString] = ctx.processSample(samples)

    logger.info(s"sample ranges: ${sampleRanges.size}")

    ctx.broadcast {
      _.classify(DataForClassify(sampleRanges, workerDests.asScala.toSeq))
    }

    logger.info(s"start final sort...")

    ctx.broadcast {
      _.finalSort()
    }

    logger.info(s"finished.")
  }
}