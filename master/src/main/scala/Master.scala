import Common.Protocol._
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
    }.to(Seq)

  samples.foreach(sample => logger.info(s"sample received: ${sample.size}"))

  val sampleRanges = ctx.processSample(samples).fold(ByteString.EMPTY)(_ concat _)

  logger.info(s"sample ranges: ${sampleRanges.size}")

  ctx.broadcast.map { worker =>
    worker.send(Classify, new Bytes(bytes = sampleRanges))
  }.to(Seq)

  ctx.broadcast.map { worker =>
    worker.send(FinalSort)
  }.to(Seq)
}