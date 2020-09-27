package Common

import java.io.File
import java.net.InetAddress

import io.undertow.util.FileUtils
import org.apache.logging.log4j.scala.Logging
import protocall.Empty

import scala.language.implicitConversions

object Util extends Logging {

  implicit def unitToEmpty(unit: Unit): Empty = new Empty

  def cleanDir(dir: File): Unit = {
    logger.info(s"clean out/")
    dir.listFiles.filter(_.getName != "log.log").foreach(_.delete)
  }

  def cleanTemp(dir: File): Unit = {
    logger.info(s"clean out/temp")
    dir.listFiles.filter(_.getName.startsWith("temp")).foreach(_.delete)
  }

  def log2(x: Double): Double = math.log(x) / math.log(2)

  def byteToUnsigned(byte: Byte): Int = byte & 0xff

  object NamedParamForced {
    /**
     * Usage:
     * class A(x: NamedParam = Forced, a: Int, ...
     */
    val Forced: NamedParam = NamedParam()

    class NamedParam private(val a: Unit) extends AnyVal

    private object NamedParam {
      def apply(): NamedParam = new NamedParam
    }

  }

  def getMyAddress: String = java.net.InetAddress.getLocalHost.getHostAddress
}