package Common

import java.io.{BufferedReader, IOException, InputStreamReader}
import java.nio.file.{Path, Paths}

import org.apache.logging.log4j.scala.Logging

// from https://stackoverflow.com/questions/934191/how-to-check-existence-of-a-program-in-the-path/38073998#38073998

object SimulationUtils extends Logging{

  def lookForProgramInPath(desiredProgram: String): Path = {
    val pb = new ProcessBuilder(if (isWindows) "where"
    else "which", desiredProgram)
    var foundProgram: Path = null
    try {
      val proc = pb.start
      val errCode = proc.waitFor
      if (errCode == 0) {
          val reader = new BufferedReader(new InputStreamReader(proc.getInputStream))
          try foundProgram = Paths.get(reader.readLine)
          finally if (reader != null) reader.close()
        logger.info(desiredProgram + " has been found at : " + foundProgram)
      }
      else
        logger.warn(desiredProgram + " not in PATH")
    } catch {
      case ex@(_: IOException | _: InterruptedException) =>
        logger.warn("Something went wrong while searching for " + desiredProgram)
    }
    foundProgram
  }

  private def isWindows = System.getProperty("os.name").toLowerCase.contains("windows")
}