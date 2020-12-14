package Worker

import java.io.File

import Common.Const.RECORD_COUNT_IN_OUT_FILE
import Common.RecordStream.RecordStream
import Common.Util.NamedParamForced._
import Common.Util.{cleanDir, cleanTemp}
import Common._
import com.google.protobuf.ByteString
import org.apache.logging.log4j.scala.Logging
import protocall.ProtoCallGrpc.ProtoCall
import protocall.{DataForClassify, DataForConnect}

import scala.collection.parallel.CollectionConverters.{ArrayIsParallelizable, seqIsParallelizable}
import scala.concurrent.ExecutionContextExecutorService

class Context(x: NamedParam = Forced, masterDest: String, in: Seq[File], out: File) extends Logging {

  private val util = new Util(masterDest=masterDest, in=in, out=out)

  implicit val ec: ExecutionContextExecutorService = util.ec

  def startServer(protoCall: ProtoCall): Unit = util.startServer(protoCall)

  def stopServer(): Unit = util.stopServer()

  def blockUntilShutdown(): Unit = util.blockUntilShutdown()

  def connectToMaster(): Unit = {
    logger.info("Connecting to master...")
    Common.RPCClient.master(masterDest).connect(DataForConnect(util.thisDest()))
  }

  def sample(): ByteString = util.sample()

  def classify(data: DataForClassify): Unit = {
    cleanDir(util.outDir)
    util.classifyThenSendOrSave(data)
  }

  def collect(data: ByteString): Unit = {
    logger.info(s"File received.")
    val path = new File(util.outDir, s"tempClassifiedByOther${util.getNextNewFileName}").toPath
    Files.write(path, data)
  }

  def finalSort(): Unit = {
    sortEachPartition()
    mergeAll()
    cleanTemp(util.outDir)
  }

  private def sortEachPartition(): Unit = {
    util.outDir.listFiles.filter(_.getName.startsWith("temp")).foreach({ file =>
      logger.info(s"sorting partition: ${file.getName}")
      val data = Files.readAll(file)
      val sorted = Sorts.mergeSort(RecordArray.from(data))
      Files.write(file.toPath, sorted.toByteString)
    })
  }

  private def mergeAll(): Unit = {
    logger.info(s"merging all partitions...")
    val paths = util.outDir.listFiles.filter(_.getName.startsWith("temp")).map(_.toPath)
    val streams = paths.map(Files.inputStreamShouldBeClosed)
    val files = streams.map(RecordStream.from)
    sortAndWritePartitions(files)
    streams.foreach(_.close())
  }
  private def sortAndWritePartitions(files: Seq[RecordStream]): Unit = {
    val sorted = Sorts.sortFromSorteds(files)
    var outFileIndex = 0

    while (sorted.hasNext) {
      val subset = sorted.take(RECORD_COUNT_IN_OUT_FILE).toSeq
      val path = new File(util.outDir, f"partition.$outFileIndex%04d").toPath
      Files.write(path, subset)
      outFileIndex += 1
    }
    println((0 until outFileIndex).map(outFileIndex => f"partition.$outFileIndex%04d").mkString(" "))
  }
}
