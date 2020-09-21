package Master

import Common.Protocol
import com.google.protobuf.empty.Empty
import scalapb.GeneratedMessage

class Worker(workerIndex: Int) {
  def send[OrderMsgType <: GeneratedMessage, ResultMsgType <: GeneratedMessage]
  (protocol: Protocol[OrderMsgType, ResultMsgType], msg: GeneratedMessage = new Empty): (Worker, ResultMsgType) =
    (this, Common.Util.send(workerIndex, protocol, msg))
}
