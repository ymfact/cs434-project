package Worker

import java.io.File
import java.util.concurrent.atomic.AtomicInteger

import Common.Const.BYTE_COUNT_IN_RECORD
import Common.RecordStream.recordsToByteString
import Common.SimulationUtils.lookForProgramInPath
import Common.Util.NamedParamForced._
import Common._
import com.google.protobuf.ByteString
import org.apache.logging.log4j.scala.Logging
import protocall.Bytes

import scala.collection.parallel.CollectionConverters.{MapIsParallelizable, seqIsParallelizable}
import scala.sys.process.Process

class Util(x: NamedParam = Forced, rootDir: File, workerCount: Int, workerIndex: Int, partitionCount: Int, partitionSize: Int, sampleCount: Int, isBinary: Boolean) extends Logging {

  type KeyType = ByteString
  val port: Int = 65400 + workerIndex
  val workerDir = new File(rootDir, s"$workerIndex")
  private val nextNewFileName: AtomicInteger = new AtomicInteger(partitionCount)

  def getNextNewFileName: Int = nextNewFileName.getAndIncrement

  def gensort(): Unit = {
    val programPath = lookForProgramInPath("gensort").toString
    (0 until partitionCount).par.foreach { partitionIndex =>
      val command = gensortCommand(programPath, partitionIndex)
      logger.info(command)
      workerDir.mkdirs()
      Process(command, workerDir).!!
    }
  }

  private def gensortCommand(programPath: String, partitionIndex: Int): String = {
    val binaryArg = if (isBinary) "" else "-a"
    val beginningRecord = (workerIndex * partitionCount + partitionIndex) * partitionSize
    s"'$programPath' $binaryArg -b$beginningRecord $partitionSize $partitionIndex"
  }

  def sample(): ByteString = {
    val path = new File(workerDir, "0").toPath
    val recordArray = Files.inputStream(path) { stream =>
      val byteArray = Files.readSome(stream, sampleCount * BYTE_COUNT_IN_RECORD)
      RecordArray.from(byteArray)
    }
    val sorted = Sorts.mergeSort(recordArray)
    sorted.toByteString
  }

  def classifyThenSendOrSave(keyRanges: Seq[KeyType]): Unit = {
    (0 until partitionCount).par.foreach { partitionIndex =>
      val path = new File(workerDir, s"$partitionIndex").toPath
      Files.inputStream(path) { stream =>
        val records = RecordStream.from(stream)
        val classified = records.groupBy(record => getOwnerOfRecord(record, keyRanges))
        for ((workerIndex, records) <- classified.par) {
          if (workerIndex == this.workerIndex) {
            val path = new File(workerDir, s"temp$partitionIndex").toPath
            Files.write(path, records)
          } else {
            val client = Common.RPCClient(workerIndex)
            client.collect(Bytes(recordsToByteString(records)))
          }
        }
      }
    }
  }

  private def getOwnerOfRecord(recordPtr: Record, keyRanges: Seq[KeyType]): Int = {
    for ((key, index) <- keyRanges.zipWithIndex)
      if (recordPtr.compare(key) < 0)
        return index
    keyRanges.length
  }
}
