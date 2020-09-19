import Common.Protocol.{Clean, Gensort, Sample}
import Master.Context
import org.apache.logging.log4j.scala.Logging

class Master(ctx: Context) extends Logging {

  logger.info(s"Initialized")

  val samples =
    ctx.broadcast.map { worker =>
      worker.send(Clean)
    }.map { case (worker, result) =>
      worker.send(Gensort)
    }.map { case (worker, result) =>
      worker.send(Sample)
    }.map { case (worker, result) =>
      result.bytes
    }.toSeq

  samples.foreach(sample => logger.info(s"received sample size: ${sample.size}"))
  ctx.processSample(samples)
}