package Master

import java.io.File

import Common.Const.SAMPLE_COUNT
import Common.{Data, RecordStream}
import com.google.protobuf.ByteString
import org.apache.logging.log4j.scala.Logging

import scala.collection.parallel.ParSeq

class Util(rootDir: File, workerCount: Int, partitionCount: Int, partitionSize: Int) extends Logging {

  val masterDir = new File(rootDir, "master")
  
  def processSample(data: ParSeq[ByteString]) = {
    val recordArrays = data.map(_.newInput).map(RecordStream.from).seq
    val sorted = Data.sortFromSorteds(recordArrays)
    sorted.grouped(SAMPLE_COUNT).map(_.head.getKeyByteString).drop(1)
  }
}
