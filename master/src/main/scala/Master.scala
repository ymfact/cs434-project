import Common.Const.BYTE_COUNT_IN_RECORD
import Common.Protocol.{Clean, Gensort, Sample, Classify}
import Master.Context
import bytes.Bytes
import com.google.protobuf.ByteString
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

  samples.foreach(sample => logger.info(s"sample received: ${sample.size}"))

  val sampleRanges = ctx.processSample(samples).fold(ByteString.EMPTY)(_ concat _)

  ctx.broadcast.map{ worker =>
    worker.send(Classify, new Bytes(bytes=sampleRanges))
  }.seq
}