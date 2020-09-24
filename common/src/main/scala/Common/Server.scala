package Common

import Common.Const.MAX_INBOUND_MESSAGE_SIZE
import io.grpc.{Server, ServerBuilder, ServerServiceDefinition}
import org.apache.logging.log4j.scala.Logger
import protocall.MasterServiceGrpc.MasterService
import protocall.{MasterServiceGrpc, ProtoCallGrpc}
import protocall.ProtoCallGrpc.ProtoCall

import scala.concurrent.ExecutionContext

object Server {
  def startMaster(masterService: MasterService, logger: Logger)(implicit ec: ExecutionContext): Server ={
    start(MasterServiceGrpc.bindService(masterService, ec), logger)
  }

  def startWorker(protoCall: ProtoCall, logger: Logger)(implicit ec: ExecutionContext): Server ={
    start(ProtoCallGrpc.bindService(protoCall, ec), logger)
  }

  private def start(service: ServerServiceDefinition, logger: Logger): Server = {
    val serverBuilder = ServerBuilder.forPort(0)
    serverBuilder.maxInboundMessageSize(MAX_INBOUND_MESSAGE_SIZE)
    serverBuilder.addService(service)
    val server = serverBuilder.build().start()
    logger.info("Server started, listening on " + server.getPort)
    sys.addShutdownHook {
      logger.warn("*** shutting down gRPC server since JVM is shutting down")
      if (server != null)
        server.shutdown()
      logger.warn("*** server shut down")
    }
    logger.info(s"Initialized")
    server
  }
}
