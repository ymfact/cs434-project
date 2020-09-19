package Worker

import java.io.File

import Common.Const.{BYTE_COUNT_IN_KEY, SAMPLE_COUNT}
import Common.RecordTypes.{ImmutableRecordArray, MutableRecordArray}
import Common.Util.cleanTemp
import Common.{Data, Protocol}
import Worker.Types.WorkerIndexType
import cask.Request
import com.google.protobuf.ByteString
import org.apache.logging.log4j.scala.Logging
import scalapb.GeneratedMessage

class Context(rootDir: File, val workerIndex: WorkerIndexType, workerCount: Int, partitionCount: Int, partitionSize: Int, isBinary: Boolean) extends Logging {

  private val util = new Util(rootDir, workerIndex, workerCount, partitionCount, partitionSize, isBinary)

  def workerDir: File = util.workerDir

  def endPoint[OrderMsgType <: GeneratedMessage, ResultMsgType <: GeneratedMessage]
  (protocol: Protocol[OrderMsgType, ResultMsgType])
  (f: OrderMsgType => ResultMsgType)
  (request: Request): Array[Byte] = {
    val orderMsg = protocol.OrderType.parseFrom(request.bytes)
    f(orderMsg).toByteArray
  }

  def clean(): Unit = Common.Util.clean(workerDir)

  def gensort(): Unit = util.gensort()

  def sample(): ByteString = util.sample()

  def classify(data: ByteString): Unit = {
    val keyCount = data.size() / BYTE_COUNT_IN_KEY
    val keyRanges = (0 until keyCount).map(index => data.substring(index * BYTE_COUNT_IN_KEY, (index + 1) * BYTE_COUNT_IN_KEY))
    util.classifyThenSendOrSave(keyRanges)
  }

  def collect(data: ByteString): Unit = {
    val path = new File(workerDir, s"temp${util.getNextNewFileName}").toPath
    Data.write(path, data)
  }

  def finalSort(): Unit ={
    sortEachPartition
    mergeAllPartitions
    cleanTemp(workerDir)
  }

  private def sortEachPartition(): Unit ={
    (0 until partitionCount * workerCount).foreach({ partitionIndex =>
      logger.info(s"sorting partition $partitionIndex")
      val path = new File(workerDir, s"temp$partitionIndex").toPath
      val data = Data.readAll(path)
      val sorted = Data.inplaceSort(MutableRecordArray.from(data))
      Data.write(path, sorted.toByteString)
    })
  }

  private def mergeAllPartitions(): Unit ={
    val partitions = (0 until partitionCount * workerCount).map({ partitionIndex =>
      val path = new File(workerDir, s"temp$partitionIndex").toPath
      ImmutableRecordArray.from(Data.readAll(path))
    })
    logger.info(s"merging all partitions")
    val sorted = Data.sortFromSorteds(partitions)
    for ((sorted, partitionIndex) <- sorted.grouped(partitionSize).zipWithIndex){
      val path = new File(workerDir, s"$partitionIndex").toPath
      val data = sorted.map(_.getByteArray).map(ByteString.copyFrom(_)).fold(ByteString.EMPTY)(_ concat _)
      Data.write(path, data)
    }
  }
}
