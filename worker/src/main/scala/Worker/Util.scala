package Worker

import java.io.File
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger

import Common.Const.{BYTE_COUNT_IN_RECORD, SAMPLE_COUNT}
import Common.RecordStream.recordsToByteString
import Common.Util.NamedParamForced._
import Common.Util.getMyAddress
import Common._
import com.google.protobuf.ByteString
import io.grpc.Server
import org.apache.logging.log4j.scala.Logging
import protocall.ProtoCallGrpc.{ProtoCall, ProtoCallStub}
import protocall.{Bytes, DataForClassify}

import scala.collection.parallel.CollectionConverters.{MapIsParallelizable, seqIsParallelizable}
import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService}

class Util(x: NamedParam = Forced, masterDest: String, in: Seq[File], out: File) extends Logging {

  implicit val ec: ExecutionContextExecutorService = ExecutionContext.fromExecutorService(Executors.newSingleThreadExecutor)

  val inFiles: Seq[File] = in.flatMap(_.listFiles())

  val outDir: File = out

  private val nextNewFileName: AtomicInteger = new AtomicInteger

  def getNextNewFileName: Int = nextNewFileName.getAndIncrement

  private var server: Server = _

  def thisDest(): String = s"$getMyAddress:${server.getPort}"

  def startServer(protoCall: ProtoCall): Unit = {
    server = Common.Server.startWorker(protoCall: ProtoCall, logger)
  }

  def stopServer(): Unit =
    if (server != null)
      server.shutdown()

  def blockUntilShutdown(): Unit =
    if(server != null)
      server.awaitTermination()

  def sample(): ByteString = {
    val path = inFiles.head.toPath
    val recordArray = Files.inputStream(path) { stream =>
      val byteArray = Files.readSome(stream, SAMPLE_COUNT * BYTE_COUNT_IN_RECORD)
      RecordArray.from(byteArray)
    }
    val sorted = Sorts.mergeSort(recordArray)
    sorted.toByteString
  }

  def classifyThenSendOrSave(data: DataForClassify): Unit = {
    val keyRanges: Seq[ByteString] = data.keys
    val clients: Seq[ProtoCallStub] = data.dests.map(RPCClient.worker)
    val thisIndex: Int = data.dests.indexOf(thisDest())
    inFiles.zipWithIndex.par.foreach { case(inFile, inFileIndex) =>
      Files.inputStream(inFile.toPath) { stream =>
        val records = RecordStream.from(stream)
        val classified = records.groupBy(record => getOwnerOfRecord(record, keyRanges))
        for ((workerIndex, records) <- classified.par) {
          if (workerIndex == thisIndex) {
            val path = new File(outDir, s"tempClassifiedMine$inFileIndex").toPath
            Files.write(path, records)
          } else {
            clients(workerIndex).collect(Bytes(recordsToByteString(records)))
          }
        }
      }
    }
  }

  private def getOwnerOfRecord(recordPtr: Record, keyRanges: Seq[ByteString]): Int = {
    for ((key, index) <- keyRanges.zipWithIndex)
      if (recordPtr.compare(key) < 0)
        return index
    keyRanges.length
  }
}
