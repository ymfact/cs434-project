import Master.Context
import com.google.protobuf.ByteString
import org.apache.logging.log4j.scala.Logging
import protocall.Bytes

import scala.concurrent.ExecutionContextExecutorService

import Common.Util.unitToEmpty

class Master(ctx: Context) extends Logging {
  implicit val ec: ExecutionContextExecutorService = ctx.ec
  logger.info(s"Initialized")

  val samples: Seq[ByteString] = ctx.broadcast { worker =>
    for {
      _ <- worker.clean()
      _ <- worker.gensort()
      result <- worker.sample()
    } yield result.bytes
  }

  samples.foreach(sample => logger.info(s"sample received: ${sample.size}"))

  val sampleRanges: ByteString = ctx.processSample(samples).fold(ByteString.EMPTY)(_ concat _)

  logger.info(s"sample ranges: ${sampleRanges.size}")

  ctx.broadcast {
    _.classify(Bytes(sampleRanges))
  }

  logger.info(s"start final sort")

  ctx.broadcast {
    _.finalSort()
  }

  logger.info(s"finished")
}