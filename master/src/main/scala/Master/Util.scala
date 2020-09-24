package Master

import java.io.{DataInputStream, File}
import java.util.concurrent.Executors

import Common.Const.BYTE_COUNT_IN_RECORD
import Common.Util.NamedParamForced._
import Common.Util.getMyAddress
import Common.{RecordStream, Sorts}
import com.google.protobuf.ByteString
import io.grpc.Server
import org.apache.logging.log4j.scala.Logging
import protocall.MasterServiceGrpc.MasterService

import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService}

class Util(x: NamedParam = Forced, workerCount: Int) extends Logging {

  implicit val ec: ExecutionContextExecutorService = ExecutionContext.fromExecutorService(Executors.newSingleThreadExecutor)

  private var server: Server = _

  def thisDest(): String = s"$getMyAddress:${server.getPort}"

  def startServer(masterService: MasterService): Unit =
    server = Common.Server.startMaster(masterService, logger)

  def stopServer(): Unit =
    if(server != null)
      server.shutdown()

  def blockUntilShutdown(): Unit =
    if(server != null)
      server.awaitTermination()

  def processSample(data: Seq[ByteString]): Seq[ByteString] = {
    val recordArrays = data.map(_.newInput).map(new DataInputStream(_)).map(RecordStream.from)
    val sorted = Sorts.sortFromSorteds(recordArrays)
    sorted.grouped(data.head.size() / BYTE_COUNT_IN_RECORD).map(_.head.copyKey).drop(1).toSeq
  }
}
