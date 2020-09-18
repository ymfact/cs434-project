package Worker

import Common.Protocol
import cask.endpoints.{ParamReader, WebEndpoint}
import cask.model.Response.Raw
import cask.model.{Request, Response}
import cask.router.{ArgReader, NoOpParser, Result}
import scalapb.GeneratedMessage

class Endpoint[OrderMsgType <: GeneratedMessage, ResultMsgType <: GeneratedMessage](val path: String, protocol: Protocol[OrderMsgType, ResultMsgType])
  extends cask.router.Endpoint[Response.Raw, ResultMsgType, Any] {
  val methods = Seq("post")
  type InputParser[T] = NoOpParser[Any, T]
  def wrapPathSegment(s: String) = Seq(s)

  def wrapFunction(ctx: Request, delegate: Delegate): Result[Response.Raw] = {
    delegate(Map("data" -> protocol.OrderType.parseFrom(ctx.bytes))).map(resultMsg => resultMsg.toByteArray)
  }
}