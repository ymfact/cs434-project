package Worker

import java.io.File
import java.util.concurrent.atomic.AtomicInteger

import Common.Const.{BYTE_COUNT_IN_RECORD, SAMPLE_COUNT}
import Common.Protocol.Collect
import Common.RecordStream.recordsToByteString
import Common.SimulationUtils.lookForProgramInPath
import Common.{Files, Record, RecordArray, RecordStream, Sorts}
import Worker.Types.WorkerIndexType
import bytes.Bytes
import com.google.protobuf.ByteString
import org.apache.logging.log4j.scala.Logging

import scala.collection.parallel.CollectionConverters.MapIsParallelizable
import scala.sys.process.Process

class Util(rootDir: File, workerIndex: WorkerIndexType, workerCount: Int, partitionCount: Int, partitionSize: Int, isBinary: Boolean) extends Logging {

  val workerDir = new File(rootDir, s"$workerIndex")
  val maxSampleCount = partitionSize / 4

  private val nextNewFileName: AtomicInteger = new AtomicInteger(partitionCount)
  def getNextNewFileName: Int = nextNewFileName.getAndIncrement

  def gensort(): Unit = {
    (0 until partitionCount).foreach { partitionIndex =>
      val command = gensortCommand(partitionIndex)
      logger.info(command)
      workerDir.mkdirs()
      Process(command, workerDir).!!
    }
  }

  private def gensortCommand(partitionIndex: Int): String = {
    val programPath = lookForProgramInPath("gensort").toString
    val binaryArg = if (isBinary) "" else "-a"
    val beginningRecord = (workerIndex * partitionCount + partitionIndex) * partitionSize
    s"'$programPath' $binaryArg -b$beginningRecord $partitionSize $partitionIndex"
  }

  def sample(): ByteString = {
    val path = new File(workerDir, "0").toPath
    val recordArray = Files.inputStream(path){ stream =>
        val byteArray = Files.readSome(stream, SAMPLE_COUNT * BYTE_COUNT_IN_RECORD)
        RecordArray.from(byteArray)
      }
    val sorted = Sorts.mergeSort(recordArray)
    sorted.toByteString
  }

  type KeyType = ByteString

  private def getOwnerOfRecord(recordPtr: Record, keyRanges: Seq[KeyType]): Int ={
    for ( (key, index) <- keyRanges.zipWithIndex)
      if(recordPtr.compare(key) < 0)
        return index
    keyRanges.length
  }

  def classifyThenSendOrSave(keyRanges: Seq[KeyType]): Unit = {
    (0 until partitionCount).foreach { partitionIndex =>
      val path = new File(workerDir, s"$partitionIndex").toPath
      Files.inputStream(path){ stream =>
        val records = RecordStream.from(stream)
        val classified = records.groupBy(record => getOwnerOfRecord(record, keyRanges))
        for( (workerIndex, records) <- classified.par) {
          if(workerIndex == this.workerIndex){
            val path = new File(workerDir, s"temp$partitionIndex").toPath
            Files.write(path, records)
          }else{
            Common.Util.send(workerIndex, Collect, new Bytes(recordsToByteString(records)))
          }
        }
      }
    }
  }
}
