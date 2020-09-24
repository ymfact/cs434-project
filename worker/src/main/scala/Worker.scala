
import Common.Util.unitToEmpty
import Worker.Context
import org.apache.logging.log4j.scala.Logging
import protocall.ProtoCallGrpc.ProtoCall
import protocall.{Bytes, DataForClassify, Empty}

import scala.concurrent.Future

class Worker(ctx: Context) extends Logging { self =>

  ctx.startServer(new ProtoCallImpl)

  ctx.connectToMaster()

  def blockUntilShutdown(): Unit = ctx.blockUntilShutdown()

  private class ProtoCallImpl extends ProtoCall {

    override def sample(request: Empty): Future[Bytes] = {
      logger.info(s"start sampling...")
      Future.successful(new Bytes(ctx.sample()))
    }

    override def classify(request: DataForClassify): Future[Empty] = {
      logger.info(s"key received: ${request.keys.length}")
      ctx.classify(request)
      Future.successful()
    }

    override def collect(request: Bytes): Future[Empty] = {
      logger.info(s"collect received: ${request.bytes.size()}")
      ctx.collect(request.bytes)
      Future.successful()
    }

    override def finalSort(request: Empty): Future[Empty] = {
      logger.info("start final sort...")
      ctx.finalSort()
      logger.info("finished.")
      ctx.stopServer()
      Future.successful()
    }
  }
}