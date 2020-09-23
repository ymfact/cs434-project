package Worker

import java.io.File

import Common.Const.BYTE_COUNT_IN_KEY
import Common.RecordStream.RecordStream
import Common.Util.NamedParamForced._
import Common.Util.cleanTemp
import Common._
import com.google.protobuf.ByteString
import org.apache.logging.log4j.scala.Logging

import scala.collection.parallel.CollectionConverters.seqIsParallelizable

class Context(x: NamedParam = Forced, rootDir: File, workerCount: Int, val workerIndex: Int, partitionCount: Int, partitionSize: Int, sampleCount: Int, isBinary: Boolean) extends Logging {

  private val util = new Util(rootDir = rootDir, workerCount = workerCount, workerIndex = workerIndex, partitionCount = partitionCount, partitionSize = partitionSize, sampleCount = sampleCount, isBinary = isBinary)

  val port: Int = util.port

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

  def finalSort(): Unit = {
    sortEachPartition()
    mergeAll()
    cleanTemp(workerDir)
  }

  private def sortEachPartition(): Unit = {
    (0 until partitionCount * workerCount).par.foreach({ partitionIndex =>
      logger.info(s"sorting partition $partitionIndex")
      val path = new File(workerDir, s"temp$partitionIndex").toPath
      val data = Files.readAll(path)
      val sorted = Sorts.mergeSort(RecordArray.from(data))
      Files.write(path, sorted.toByteString)
    })
  }

  def workerDir: File = util.workerDir

  private def mergeAll(): Unit = {
    logger.info(s"merging all partitions")
    val fileCount = partitionCount * workerCount
    val paths = (0 until fileCount).map(fileIndex => new File(workerDir, s"temp$fileIndex").toPath)
    val streams = paths.map(Files.inputStreamShouldBeClosed)
    val files = streams.map(RecordStream.from)
    sortAndWritePartitions(files)
    streams.foreach(_.close())
  }
  private def sortAndWritePartitions(files: Seq[RecordStream]): Unit = {
    val sorted = Sorts.sortFromSorteds(files)
    for ((sorted, partitionIndex) <- sorted.grouped(partitionSize).toSeq.par.zipWithIndex) {
      val path = new File(workerDir, s"$partitionIndex").toPath
      Files.write(path, sorted)
    }
  }
}
