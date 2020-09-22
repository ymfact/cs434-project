package Master

import java.io.{DataInputStream, File}

import Common.Const.BYTE_COUNT_IN_RECORD
import Common.Util.NamedParamForced._
import Common.{RecordStream, Sorts}
import com.google.protobuf.ByteString
import org.apache.logging.log4j.scala.Logging

class Util(x: NamedParam = Forced, rootDir: File, workerCount: Int, partitionCount: Int, partitionSize: Int) extends Logging {

  val masterDir = new File(rootDir, "master")

  def processSample(data: Seq[ByteString]): Seq[ByteString] = {
    val recordArrays = data.map(_.newInput).map(new DataInputStream(_)).map(RecordStream.from)
    val sorted = Sorts.sortFromSorteds(recordArrays)
    sorted.grouped(data.head.size() / BYTE_COUNT_IN_RECORD).map(_.head.copyKey).drop(1).toSeq
  }
}
