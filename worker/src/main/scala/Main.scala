import cask.{MainRoutes, Request}
import hello.Hello
import org.apache.logging.log4j.scala.Logging


object Main extends MainRoutes with Logging {

  @cask.post("/")
  def root(request: Request) : Array[Byte] = {
    val response = request.bytes
    val hello = Hello.parseFrom(response)
    logger.info(s"Hello from ${hello.from}")
    new Hello("Worker").toByteArray
  }

  override def port = 65400
  initialize()

  logger.info(s"Initialized")
}