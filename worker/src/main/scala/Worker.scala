
import Common.Protocol
import Worker.{Context, Endpoint}
import cask.{MainRoutes, Request}
import com.google.protobuf.ByteString
import com.google.protobuf.empty.Empty
import org.apache.logging.log4j.scala.Logging
import records.Records

import Common.Util.unitToEmpty

class Worker(ctx: Context) extends MainRoutes with Logging {

  override def port: Int = 65400 + ctx.workerIndex


  @cask.post("/clean")
  def clean(request: Request): Array[Byte] = ctx.endPoint(Protocol.Clean) (_ => {
    ctx.clean()
  })(request)

  @Endpoint("/gensort", Protocol.Gensort)
  def gensort(request: Request): Empty = {
    ctx.gensort()
  }

  @Endpoint("/sample", Protocol.Sample)
  def sample(request: Request): Records = {
    new Records(Seq[ByteString]())
  }

  def close() {
    this.finalize()
  }

  initialize()
  main(Array())
  logger.info(s"Initialized")
}