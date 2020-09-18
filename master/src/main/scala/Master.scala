import Common.Protocol.{Clean, Gensort, Sample}
import Master.Context
import com.google.protobuf.ByteString
import org.apache.logging.log4j.scala.Logging

class Master(ctx: Context) extends Logging {

  logger.info(s"Initialized")

  val sample =
    ctx.broadcast.map { worker =>
      worker.send(Clean)
    }.map { case (worker, result) =>
      worker.send(Gensort)
    }.map { case (worker, result) =>
      worker.send(Sample)
    }.seq.map { case (worker, result) =>
      result.bytes
    }.fold(ByteString.EMPTY)(_ concat _)

  logger.info(s"received sample size: ${sample.size}")
}