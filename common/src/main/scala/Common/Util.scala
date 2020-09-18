package Common

import java.io.File

import org.apache.logging.log4j.scala.Logging
import io.undertow.util.FileUtils

object Util extends Logging {
  def clean(dir: File){
    logger.info("clean")
    FileUtils.deleteRecursive(dir.toPath)
  }
}
