import cask.{Request, MainRoutes}
import hello.Hello
import org.apache.logging.log4j.scala.Logging

class Worker(index: Int) extends MainRoutes with Logging {
  override def port = 65400 + index

  @cask.post("/")
  def root(request: Request) : Array[Byte] = {
    val response = request.bytes
    val hello = Hello.parseFrom(response)
    logger.info(s"Hello from ${hello.from}")
    new Hello("Worker").toByteArray
  }

  def close() {
    this.finalize()
  }

  initialize()
  main(Array())
  logger.info(s"Initialized")
}