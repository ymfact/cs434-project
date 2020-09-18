package Worker

import Common.Protocol
import cask.endpoints.{QueryParamReader, WebEndpoint}
import cask.Request
import cask.model.Response.Raw
import cask.router.Result
import scalapb.GeneratedMessage

class Endpoint[OrderMsgType <: GeneratedMessage, ResultMsgType <: GeneratedMessage](val path: String, protocol: Protocol[OrderMsgType, ResultMsgType])
  extends cask.router.Endpoint[Raw, ResultMsgType, Seq[String]]{

  implicit def RequestToMsg(request: Request): OrderMsgType = protocol.OrderType.parseFrom(request.bytes)

  type InputParser[T] = QueryParamReader[T]
  def wrapPathSegment(s: String) = Seq(s)
  val methods = Seq("post")

  def wrapFunction(ctx: cask.Request, delegate: Delegate): Result[Raw] = {
    delegate(WebEndpoint.buildMapFromQueryParams(ctx)).map(resultMsg => resultMsg.toByteArray)
  }
}