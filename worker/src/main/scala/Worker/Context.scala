package Worker

import java.io.File

import Common.Const.BYTE_COUNT_IN_KEY
import Common.{Data, Protocol}
import Worker.Types.WorkerIndexType
import cask.Request
import com.google.protobuf.ByteString
import scalapb.GeneratedMessage

class Context(rootDir: File, val workerIndex: WorkerIndexType, workerCount: Int, partitionCount: Int, partitionSize: Int, isBinary: Boolean) {

  private val util = new Util(rootDir, workerIndex, workerCount, partitionCount, partitionSize, isBinary)

  def workerDir: File = util.workerDir

  def endPoint[OrderMsgType <: GeneratedMessage, ResultMsgType <: GeneratedMessage]
  (protocol: Protocol[OrderMsgType, ResultMsgType])
  (f: OrderMsgType => ResultMsgType)
  (request: Request): Array[Byte] = {
    val orderMsg = protocol.OrderType.parseFrom(request.bytes)
    f(orderMsg).toByteArray
  }

  def clean(): Unit = Common.Util.clean(workerDir)

  def gensort(): Unit = util.gensort()

  def sample(): ByteString = util.sample()

  def classify(data: ByteString): Unit = {
    val keyCount = data.size() / BYTE_COUNT_IN_KEY
    val keyRanges = (0 until keyCount).map(index => data.substring(index * BYTE_COUNT_IN_KEY, (index + 1) * BYTE_COUNT_IN_KEY))
    util.classifyThenSendOrSave(keyRanges)
  }

  def collect(data: ByteString): Unit = {
    val path = new File(workerDir, s"${util.getNextNewFileName}").toPath
    Data.write(path, data)
  }
}
