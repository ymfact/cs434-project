
import Common.Protocol._
import Common.Util.unitToEmpty
import Worker.{Context, Endpoint}
import bytes.Bytes
import cask.MainRoutes
import com.google.protobuf.empty.Empty
import org.apache.logging.log4j.scala.Logging

class Worker(ctx: Context) extends MainRoutes with Logging {

  override def port: Int = 65400 + ctx.workerIndex


  @Endpoint(Clean)
  def clean(data: Empty): Empty = {
    ctx.clean()
  }

  @Endpoint(Gensort)
  def gensort(data: Empty): Empty = {
    ctx.gensort()
  }

  @Endpoint(Sample)
  def sample(data: Empty): Bytes = {
    new Bytes(ctx.sample())
  }

  @Endpoint(Classify)
  def classify(data: Bytes): Empty = {
    logger.info(s"key received: ${data.bytes.size()}")
    ctx.classify(data.bytes)
  }

  @Endpoint(Collect)
  def collect(data: Bytes): Empty = {
    logger.info(s"collect received: ${data.bytes.size()}")
    ctx.collect(data.bytes)
  }

  @Endpoint(FinalSort)
  def finalSort(data: Empty): Empty = {
    ctx.finalSort()
    close()
  }

  def close() {
    new Thread(() => {
      Thread.sleep(1000)
      System.exit(0)
    }).start()
  }

  initialize()
  main(Array())
  logger.info(s"Initialized")
}