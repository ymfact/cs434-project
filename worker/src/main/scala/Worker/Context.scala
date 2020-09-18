package Worker

import java.io.File

import Common.Protocol
import Worker.Types.WorkerIndexType
import cask.Request
import com.google.protobuf.ByteString
import scalapb.GeneratedMessage

class Context(dir: File, val workerIndex: WorkerIndexType, partitionCount: Int, partitionSize: Int, isBinary: Boolean) {

  private val util = new Util(dir, workerIndex, partitionCount, partitionSize, isBinary)

  def workerDir: File = util.workerDir

  def endPoint[OrderMsgType <: GeneratedMessage, ResultMsgType <: GeneratedMessage]
  (protocol: Protocol[OrderMsgType, ResultMsgType])
  (f: OrderMsgType => ResultMsgType)
  (request: Request): Array[Byte] = {
    val orderMsg = protocol.OrderType.parseFrom(request.bytes)
    f(orderMsg).toByteArray
  }

  def clean(): Unit = util.clean()

  def gensort(): Unit = util.gensort()

  def sample(): ByteString = util.sample()
}
