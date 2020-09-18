package Worker

import java.io.File

import Common.Const.BYTE_COUNT_IN_RECORD
import Common.SimulationUtils.lookForProgramInPath
import Worker.Types.WorkerIndexType
import com.google.protobuf.ByteString
import org.apache.logging.log4j.scala.Logging

import scala.io.Source
import scala.sys.process.Process

class Util(rootDir: File, workerIndex: WorkerIndexType, partitionCount: Int, partitionSize: Int, isBinary: Boolean) extends Logging {

  val workerDir = new File(rootDir, s"$workerIndex")

  def clean(): Unit = Common.Util.clean(workerDir)

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
    val len = (partitionSize / 4) * BYTE_COUNT_IN_RECORD
    val fileName = new File(workerDir, "0")
    val stream = Source.fromFile(fileName).bufferedReader()
    val seq = LazyList.continually(stream.read).map(_.toByte).take(len)
    ByteString.copyFrom(seq.toArray)
  }
}
