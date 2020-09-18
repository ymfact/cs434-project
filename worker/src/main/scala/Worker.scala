
import Common.Protocol
import Worker.{Context, Endpoint}
import cask.MainRoutes
import com.google.protobuf.ByteString
import com.google.protobuf.empty.Empty
import org.apache.logging.log4j.scala.Logging
import records.Records

import Common.Util.unitToEmpty

class Worker(ctx: Context) extends MainRoutes with Logging {

  override def port: Int = 65400 + ctx.workerIndex


  @Endpoint("/clean", Protocol.Clean)
  def clean(data: Empty): Empty = {
    ctx.clean()
  }

  @Endpoint("/gensort", Protocol.Gensort)
  def gensort(data: Empty): Empty = {
    ctx.gensort()
  }

  @Endpoint("/sample", Protocol.Sample)
  def sample(data: Empty): Records = {
    new Records(Seq[ByteString]())
  }

  def close() {
    this.finalize()
  }

  initialize()
  main(Array())
  logger.info(s"Initialized")
}