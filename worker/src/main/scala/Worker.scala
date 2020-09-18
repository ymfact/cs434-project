
import Worker.Context
import cask.{MainRoutes, Request}
import hello.Hello
import org.apache.logging.log4j.scala.Logging

class Worker(ctx: Context) extends MainRoutes with Logging {
  import ctx._

  override def port: Int = 65400 + workerIndex

  @cask.post("/")
  def root(request: Request) : Array[Byte] = {
    val response = request.bytes
    val hello = Hello.parseFrom(response)
    logger.info(s"Hello from ${hello.from}")
    gensort()
    new Hello("Worker").toByteArray
  }

  def close() {
    this.finalize()
  }

  initialize()
  main(Array())
  logger.info(s"Initialized")

  clean()
}