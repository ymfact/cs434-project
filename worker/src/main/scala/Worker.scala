
import bytes.Bytes
import Common.Protocol
import Worker.{Context, Endpoint}
import cask.MainRoutes
import com.google.protobuf.empty.Empty
import org.apache.logging.log4j.scala.Logging
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
  def sample(data: Empty): Bytes = {
    new Bytes(ctx.sample)
  }

  @Endpoint("/sample_result", Protocol.SampleResult)
  def sample_result(data: Bytes): Empty = {
    logger.info(s"key received: ${data.bytes}")
  }

  def close() {
    this.finalize()
  }

  initialize()
  main(Array())
  logger.info(s"Initialized")
}