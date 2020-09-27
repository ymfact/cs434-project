package Master

import java.io.DataInputStream

import Common.Const.BYTE_COUNT_IN_RECORD
import Common.Util.NamedParamForced._
import Common.Util.unitToEmpty
import Common.{RPCClient, RecordStream, Sorts}
import com.google.protobuf.ByteString
import org.apache.logging.log4j.scala.Logging
import protocall.ProtoCallGrpc

class Util(x: NamedParam = Forced, val workerDests: Seq[String]) extends Logging {

  val workers: Seq[ProtoCallGrpc.ProtoCallStub] = workerDests.map(RPCClient.worker)

  def processSample(data: Seq[ByteString]): Seq[ByteString] = {
    val recordArrays = data.map(_.newInput).map(new DataInputStream(_)).map(RecordStream.from)
    val sorted = Sorts.sortFromSorteds(recordArrays)
    sorted.grouped(data.head.size() / BYTE_COUNT_IN_RECORD).map(_.head.copyKey).drop(1).toSeq
  }
}
