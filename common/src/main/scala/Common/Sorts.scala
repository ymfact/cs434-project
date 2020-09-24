package Common

import Common.Const.BYTE_COUNT_IN_RECORD
import Common.RecordStream.RecordStream
import Common.Util.log2

import scala.collection.mutable
import scala.collection.parallel.CollectionConverters.seqIsParallelizable

object Sorts {
  def sortFromSorteds(streams: Seq[RecordStream]): Iterator[RecordFromStream] = new Iterator[RecordFromStream] {
    type Pair = (RecordFromStream, Iterator[RecordFromStream])
    implicit def comparator: Ordering[Pair] = (l: Pair, r: Pair) => -l._1.compare(r._1)
    private val queue = mutable.PriorityQueue[Pair](streams.map(_.iterator).map(iter => iter.next() -> iter): _*)

    override def hasNext: Boolean = queue.nonEmpty

    override def next(): RecordFromStream = {
      val (record, arrayIter) = queue.dequeue
      if (arrayIter.hasNext)
        queue.enqueue(arrayIter.next() -> arrayIter)
      record
    }
  }

  def mergeSort(records: RecordArray): RecordArray = {
    val len = records.length
    val extraBuffer = Array.ofDim[Byte](len * BYTE_COUNT_IN_RECORD)
    val extra = new RecordArray(extraBuffer)
    val stepCount = log2(len).ceil.toInt
    var srcDest = (records, extra)
    def src: RecordArray = srcDest._1
    def dest: RecordArray = srcDest._2

    for (step <- 0 until stepCount) {
      mergeSort(src, dest, step)
      srcDest = srcDest.swap
    }
    src
  }

  private def mergeSort(src: RecordArray, dest: RecordArray, step: Int): Unit = {
    val chunkSize = 1 << step
    (0 until src.length by 2 * chunkSize).par.foreach(begin => {
      val mid = src.length min (begin + chunkSize)
      val end = src.length min (begin + chunkSize + chunkSize)
      mergeSortConquer(src, dest, begin, mid, end)
    })
  }

  private def mergeSortConquer(src: RecordArray, dest: RecordArray, begin: Int, mid: Int, end: Int): Unit = {
    var leftCursor = begin
    var rightCursor = mid
    val leftEnd = mid
    val rightEnd = end
    var destCursor = begin
    while (true) {
      if (leftCursor >= leftEnd) {
        dest.patchInPlace(destCursor, src, rightCursor, rightEnd - rightCursor)
        return
      } else if (rightCursor >= rightEnd) {
        dest.patchInPlace(destCursor, src, leftCursor, leftEnd - leftCursor)
        return
      }
      else if (src.compareRecord(leftCursor, rightCursor) < 0) {
        dest.patchInPlace(destCursor, src, leftCursor)
        leftCursor += 1
      } else {
        dest.patchInPlace(destCursor, src, rightCursor)
        rightCursor += 1
      }
      destCursor += 1
    }
  }
}
