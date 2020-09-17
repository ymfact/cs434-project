import hello.Hello
import org.apache.logging.log4j.scala.Logging
import scalaj.http.Http
import scalapb.GeneratedMessage
import scala.collection.parallel.CollectionConverters._

class Master(worker_count:Int) extends Logging {

  val TIMEOUT_MS = 60 * 60 * 1000

  def send(index: Int, data: GeneratedMessage): Array[Byte] = {
    val port = 65400 + index
    Http(s"http://localhost:${port}/")
      .postData(data.toByteArray)
      .timeout(TIMEOUT_MS, TIMEOUT_MS)
      .asBytes
      .body
  }

  logger.info(s"Initialized")

  (0 until worker_count).par.foreach { worker_index =>
    val myHello = new Hello("Master")
    val response = send(worker_index, myHello)
    val gotHello = Hello.parseFrom(response)
    logger.info(s"Hello from ${gotHello.from}")
  }
}