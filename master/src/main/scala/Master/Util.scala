package Master

import java.io.{DataInputStream, File}

import Common.Const.{BYTE_COUNT_IN_RECORD, SAMPLE_COUNT}
import Common.Util.NamedParamForced._
import Common.{Files, RecordStream, Sorts}
import com.google.protobuf.ByteString
import org.apache.logging.log4j.scala.Logging

import scala.collection.parallel.ParSeq

class Util(x:NamedParam = Forced, rootDir: File, workerCount: Int, partitionCount: Int, partitionSize: Int) extends Logging {

  val masterDir = new File(rootDir, "master")
  
  def processSample(data: ParSeq[ByteString]) = {
    val recordArrays = data.map(_.newInput).map(new DataInputStream(_)).map(RecordStream.from).seq
    val sorted = Sorts.sortFromSorteds(recordArrays)
    sorted.grouped(data(0).size() / BYTE_COUNT_IN_RECORD).map(_.head.getKeyByteString).drop(1)
  }
}
