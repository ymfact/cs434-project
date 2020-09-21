package Worker

import java.io.File

import Common.Const.BYTE_COUNT_IN_KEY
import Common.Util.NamedParamForced._
import Common.Util.cleanTemp
import Common.{Files, Protocol, RecordArray, RecordStream, Sorts}
import cask.Request
import com.google.protobuf.ByteString
import org.apache.logging.log4j.scala.Logging
import scalapb.GeneratedMessage

import scala.collection.parallel.CollectionConverters.seqIsParallelizable

class Context(x:NamedParam = Forced, rootDir: File, workerCount: Int, val workerIndex: Int, partitionCount: Int, partitionSize: Int, sampleCount: Int, isBinary: Boolean) extends Logging {

  private val util = new Util(rootDir=rootDir, workerCount=workerCount, workerIndex=workerIndex, partitionCount=partitionCount, partitionSize=partitionSize, sampleCount=sampleCount, isBinary=isBinary)

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
    Files.write(path, data)
  }

  def finalSort(): Unit ={
    sortEachPartition()
    mergeAllPartitions()
    cleanTemp(workerDir)
  }

  private def sortEachPartition(): Unit ={
    (0 until partitionCount * workerCount).par.foreach({ partitionIndex =>
      logger.info(s"sorting partition $partitionIndex")
      val path = new File(workerDir, s"temp$partitionIndex").toPath
      val data = Files.readAll(path)
      val sorted = Sorts.mergeSort(RecordArray.from(data))
      Files.write(path, sorted.toByteString)
    })
  }

  private def mergeAllPartitions(): Unit ={
    val streams = (0 until partitionCount * workerCount).map({ partitionIndex =>
      val path = new File(workerDir, s"temp$partitionIndex").toPath
      Files.inputStreamShouldBeClosed(path)
    })
    logger.info(s"merging all partitions")
    val partitions = streams.map(RecordStream.from)
    val sorted = Sorts.sortFromSorteds(partitions)
    for ((sorted, partitionIndex) <- sorted.grouped(partitionSize).to(LazyList).par.zipWithIndex){
      val path = new File(workerDir, s"$partitionIndex").toPath
      val data = sorted.map(_.raw).map(ByteString.copyFrom).fold(ByteString.EMPTY)(_ concat _)
      Files.write(path, data)
    }
    streams.foreach(_.close())
  }
}
