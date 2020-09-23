
import Worker.Context
import io.grpc.{Server, ServerBuilder}
import org.apache.logging.log4j.scala.Logging
import protocall.{Bytes, Empty, ProtoCallGrpc}

import scala.concurrent.{ExecutionContext, Future}

import Common.Util.unitToEmpty

class Worker(executionContext: ExecutionContext, ctx: Context) extends Logging { self =>

  private[this] var server: Server = _

  def start(): Unit = {
    val serverBuilder = ServerBuilder.forPort(ctx.port)
    serverBuilder.addService(ProtoCallGrpc.bindService(new GreeterImpl, executionContext))
    server = serverBuilder.build().start()
    logger.info("Server started, listening on " + ctx.port)
    sys.addShutdownHook {
      System.err.println("*** shutting down gRPC server since JVM is shutting down")
      self.stop()
      System.err.println("*** server shut down")
    }
    logger.info(s"Initialized")
  }

  private def stop(): Unit = {
    if (server != null) {
      server.shutdown()
    }
  }

  def blockUntilShutdown(): Unit = {
    if (server != null) {
      server.awaitTermination()
    }
  }

  private class GreeterImpl extends ProtoCallGrpc.ProtoCall {
    override def clean(request: Empty): Future[Empty] = {
      ctx.clean()
      Future.successful()
    }

    override def gensort(request: Empty): Future[Empty] = {
      ctx.gensort()
      Future.successful()
    }

    override def sample(request: Empty): Future[Bytes] = {
      Future.successful(new Bytes(ctx.sample()))
    }

    override def classify(request: Bytes): Future[Empty] = {
      logger.info(s"key received: ${request.bytes.size()}")
      ctx.classify(request.bytes)
      Future.successful()
    }

    override def collect(request: Bytes): Future[Empty] = {
      logger.info(s"collect received: ${request.bytes.size()}")
      ctx.collect(request.bytes)
      Future.successful()
    }

    override def finalSort(request: Empty): Future[Empty] = {
      logger.info("start final sort")
      ctx.finalSort()
      new Thread(() => {
        Thread.sleep(1000)
        self.stop()
      }).start()
      Future.successful()
    }
  }
}