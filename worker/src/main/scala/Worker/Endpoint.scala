package Worker

import Common.Protocol
import cask.model.{Request, Response}
import cask.router.{NoOpParser, Result}
import scalapb.GeneratedMessage

class Endpoint[OrderMsgType <: GeneratedMessage, ResultMsgType <: GeneratedMessage](protocol: Protocol[OrderMsgType, ResultMsgType])
  extends cask.router.Endpoint[Response.Raw, ResultMsgType, Any] {
  type InputParser[T] = NoOpParser[Any, T]
  val path = s"/${protocol.endpoint}"
  val methods = Seq("post")

  def wrapPathSegment(s: String) = Seq(s)

  def wrapFunction(ctx: Request, delegate: Delegate): Result[Response.Raw] = {
    delegate(Map("data" -> protocol.OrderType.parseFrom(ctx.bytes))).map(resultMsg => resultMsg.toByteArray)
  }
}