package Worker

import Common.SimulationUtils.lookForProgramInPath
import io.undertow.util.FileUtils
import org.apache.logging.log4j.scala.Logging

import scala.sys.process.Process

class Util(ctx: Context) extends Logging {
  import ctx._

  def clean(){
    logger.info("clean")
    FileUtils.deleteRecursive(workerDir.toPath)
  }

  def gensort() {
    (0 until partitionCount).foreach { partitionIndex =>
      val command = gensortCommand(partitionIndex)
      logger.info(command)
      workerDir.mkdirs()
      val result = Process(command, dir).!!
      logger.info(result)
    }
  }

  private def gensortCommand(partitionIndex: Int) = {
    val programPath = lookForProgramInPath("gensort").toString
    val binaryArg = if(isBinary) "" else "-a"
    val beginningRecord = (workerIndex * partitionCount + partitionIndex) * partitionSize
    val fileName = s"$workerIndex/$partitionIndex"
    s"'$programPath' $binaryArg -b$beginningRecord $partitionSize $fileName"
  }
}
