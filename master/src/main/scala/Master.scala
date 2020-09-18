import Common.Protocol.{Clean, Gensort, Sample}
import Master.{Context, Worker}
import org.apache.logging.log4j.scala.Logging

class Master(ctx: Context) extends Logging {

  logger.info(s"Initialized")

  ctx.broadcast.map { worker =>
    worker.send(Clean)
  } map { case (worker, result) =>
    worker.send(Gensort)
  } map { case (worker, result) =>
    worker.send(Sample)
  } map { case (worker, result) =>

  }
}