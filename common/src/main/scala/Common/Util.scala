package Common

import java.io.File

import io.undertow.util.FileUtils
import org.apache.logging.log4j.scala.Logging
import protocall.Empty

import scala.language.implicitConversions

object Util extends Logging {

  def clean(dir: File) {
    logger.info("clean")
    FileUtils.deleteRecursive(dir.toPath)
  }

  implicit def unitToEmpty(unit: Unit): Empty = new Empty

  def cleanTemp(dir: File): Unit = {
    logger.info(s"clean temp*")
    dir.listFiles.filter(_.getName.startsWith("temp")).foreach(_.delete)
  }

  def log2(x: Double): Double = math.log(x) / math.log(2)

  def byteToUnsigned(byte: Byte): Int = byte & 0xff

  object NamedParamForced {

    val Forced = new NamedParam(42)

    class NamedParam(val i: Int) extends AnyVal
  }

}