import Common.Const.BYTE_COUNT_IN_RECORD
import Common.Protocol.{Clean, Gensort, Sample, SampleResult}
import Master.Context
import bytes.Bytes
import org.apache.logging.log4j.scala.Logging

import scala.collection.parallel.CollectionConverters.seqIsParallelizable
import scala.collection.parallel.ParIterable

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

  samples.foreach(sample => logger.info(s"received sample count: ${sample.size / BYTE_COUNT_IN_RECORD}"))

  val sampleResults = ctx.processSample(samples)

  ctx.broadcast.zip(sampleResults.toSeq.par).map{ case (worker, sampleResult) =>
    worker.send(SampleResult, new Bytes(bytes=sampleResult))
  }.seq
}