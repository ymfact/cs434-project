import Master.Context
import hello.Hello
import org.apache.logging.log4j.scala.Logging

class Master(ctx: Context) extends Logging {

  logger.info(s"Initialized")

  ctx.broadcast{ workerIndex =>
    val myHello = new Hello("Master")
    ctx.send(workerIndex, myHello)
  }.map{response =>
    val hello = response(Hello)
    logger.info(s"Hello from ${hello.from}")
  }
}