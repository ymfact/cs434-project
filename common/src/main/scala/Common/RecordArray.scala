package Common

import Common.Const.BYTE_COUNT_IN_RECORD
import com.google.protobuf.ByteString

import scala.collection.mutable
import scala.jdk.CollectionConverters.IteratorHasAsScala

class RecordArray (buffer: mutable.Buffer[Byte]) extends mutable.Seq[RecordArrayPtr] {
  override def update(idx: Int, elem: RecordArrayPtr): Unit = update(idx, elem.raw)

  private def update(idx: Int, elem: IterableOnce[Byte]): Unit = buffer.patchInPlace(idx * BYTE_COUNT_IN_RECORD, elem, BYTE_COUNT_IN_RECORD)

  override def apply(i: Int): RecordArrayPtr = new RecordArrayPtr(buffer, i)

  override def length: Int = buffer.length / BYTE_COUNT_IN_RECORD

  class Iterator extends scala.Iterator[RecordArrayPtr] {
    var currentIndex: Int = 0

    override def hasNext: Boolean = currentIndex < RecordArray.this.length

    override def next(): RecordArrayPtr = {
      val next = new RecordArrayPtr(buffer, currentIndex)
      currentIndex += 1
      next
    }
  }

  override def iterator: scala.Iterator[RecordArrayPtr] = new Iterator

  def toByteString: ByteString = {
    val iter = buffer.iterator
    ByteString.readFrom(() =>
      if (iter.hasNext)
        iter.next()
      else
        -1
    )
  }

  def data: mutable.Buffer[Byte] = buffer
}

object RecordArray {
  def from(byteString: ByteString): RecordArray = from(byteString.iterator.asScala.map(_.toByte))

  def from(array: Array[Byte]): RecordArray = from(array.iterator)

  private def from(iter: Iterator[Byte]): RecordArray = {
    val buffer = mutable.ArrayBuffer.fill[Byte](iter.length)(0)
    buffer.patchInPlace(0, new IterableOnce[Byte]{
      override def iterator: Iterator[Byte] = iter
    }, iter.length)
    new RecordArray(buffer)
  }
}
