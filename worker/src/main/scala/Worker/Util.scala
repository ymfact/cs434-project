package Worker

import java.io.File
import java.nio.file.Path

import Common.Const.{BYTE_COUNT_IN_RECORD, SAMPLE_COUNT}
import Common.Data.readSome
import Common.SimulationUtils.lookForProgramInPath
import Worker.Types.WorkerIndexType
import com.google.protobuf.ByteString
import org.apache.logging.log4j.scala.Logging

import scala.sys.process.Process

class Util(rootDir: File, workerIndex: WorkerIndexType, partitionCount: Int, partitionSize: Int, isBinary: Boolean) extends Logging {

  val workerDir = new File(rootDir, s"$workerIndex")
  val maxSampleCount = (partitionSize / 4)

  def gensort(): Unit = {
    (0 until partitionCount).foreach { partitionIndex =>
      val command = gensortCommand(partitionIndex)
      logger.info(command)
      workerDir.mkdirs()
      val result = Process(command, workerDir).!!
    }
  }

  private def gensortCommand(partitionIndex: Int): String = {
    val programPath = lookForProgramInPath("gensort").toString
    val binaryArg = if (isBinary) "" else "-a"
    val beginningRecord = (workerIndex * partitionCount + partitionIndex) * partitionSize
    s"'$programPath' $binaryArg -b$beginningRecord $partitionSize $partitionIndex"
  }

  def sample(): ByteString = {
    val len = math.min(SAMPLE_COUNT, (partitionSize / 4)) * BYTE_COUNT_IN_RECORD
    val path = new File(workerDir, "0").toPath
    readSome(path, len)
  }
}
