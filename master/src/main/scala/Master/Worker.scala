package Master

import Common.Protocol
import com.google.protobuf.empty.Empty
import scalapb.{GeneratedMessage, GeneratedMessageCompanion}

class Worker (util: Util, workerIndex: Int) {
  def send[OrderMsgType <: GeneratedMessage, ResultMsgType <: GeneratedMessage]
  (protocol: Protocol[OrderMsgType, ResultMsgType], msg: GeneratedMessage = new Empty): (Worker, Object {
    def apply[ReturnType <: GeneratedMessage](returnType: GeneratedMessageCompanion[ReturnType]): ReturnType
  }) = (this, util.send(workerIndex, protocol.endpoint, msg))
}
