package Common

import com.google.protobuf.empty.Empty
import records.Records
import scalapb.GeneratedMessage
import scalapb.GeneratedMessageCompanion

trait Protocol[OrderMsgType <: GeneratedMessage, ResultMsgType <: GeneratedMessage]{
  val OrderType: GeneratedMessageCompanion[OrderMsgType]
  val ResultType: GeneratedMessageCompanion[ResultMsgType]
  val endpoint: String
}

object Protocol {
  object Clean extends Protocol[Empty, Empty]{
    val OrderType: Empty.type = Empty
    val ResultType: Empty.type = Empty
    val endpoint: String = "clean"
  }
  object Gensort extends Protocol[Empty, Empty]{
    val OrderType: Empty.type = Empty
    val ResultType: Empty.type = Empty
    val endpoint: String = "gensort"
  }
  object Sample extends Protocol[Empty, Records]{
    val OrderType: Empty.type = Empty
    val ResultType: Records.type = Records
    val endpoint: String = "sample"
  }
}