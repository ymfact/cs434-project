package Common

import bytes.Bytes
import com.google.protobuf.empty.Empty
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
  object Sample extends Protocol[Empty, Bytes]{
    val OrderType: Empty.type = Empty
    val ResultType: Bytes.type = Bytes
    val endpoint: String = "sample"
  }
  object Classify extends Protocol[Bytes, Empty]{
    val OrderType: Bytes.type = Bytes
    val ResultType: Empty.type = Empty
    val endpoint: String = "classify"
  }
  object Collect extends Protocol[Bytes, Empty]{
    val OrderType: Bytes.type = Bytes
    val ResultType: Empty.type = Empty
    val endpoint: String = "collect"
  }
  object FinalSort extends Protocol[Empty, Empty]{
    val OrderType: Empty.type = Empty
    val ResultType: Empty.type = Empty
    val endpoint: String = "final_sort"
  }
}