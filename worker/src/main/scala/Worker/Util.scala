package Worker

import java.io.File

import Common.SimulationUtils.lookForProgramInPath
import Worker.Types.WorkerIndexType
import org.apache.logging.log4j.scala.Logging

import scala.sys.process.Process

class Util(dir: File, workerIndex: WorkerIndexType, partitionCount: Int, partitionSize: Int, isBinary: Boolean) extends Logging {

  val workerDir = new File(dir, s"$workerIndex")

  def clean(): Unit = Common.Util.clean(workerDir)

  def gensort() {
    (0 until partitionCount).foreach { partitionIndex =>
      val command = gensortCommand(partitionIndex)
      logger.info(command)
      workerDir.mkdirs()
      val result = Process(command, dir).!!
      logger.info(result)
    }
  }

  private def gensortCommand(partitionIndex: Int): String = {
    val programPath = lookForProgramInPath("gensort").toString
    val binaryArg = if (isBinary) "" else "-a"
    val beginningRecord = (workerIndex * partitionCount + partitionIndex) * partitionSize
    val fileName = s"$workerIndex/$partitionIndex"
    s"'$programPath' $binaryArg -b$beginningRecord $partitionSize $fileName"
  }
}
