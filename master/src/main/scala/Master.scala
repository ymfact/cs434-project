import Master.Context
import hello.Hello
import org.apache.logging.log4j.scala.Logging
import scalaj.http.Http
import scalapb.GeneratedMessage

import scala.collection.parallel.CollectionConverters._

class Master(ctx: Context) extends Logging {

  val TIMEOUT_MS = 60 * 60 * 1000

  def send(workerIndex: Int, msg: GeneratedMessage): Array[Byte] = {
    val port = 65400 + workerIndex
    Http(s"http://localhost:${port}/")
      .postData(msg.toByteArray)
      .timeout(TIMEOUT_MS, TIMEOUT_MS)
      .asBytes
      .body
  }

  logger.info(s"Initialized")

  (0 until ctx.workerCount).par.foreach { worker_index =>
    val myHello = new Hello("Master")
    val response = send(worker_index, myHello)
    val gotHello = Hello.parseFrom(response)
    logger.info(s"Hello from ${gotHello.from}")
  }
}