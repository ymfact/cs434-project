package Common

import Common.Const.BYTE_COUNT_IN_RECORD
import Common.Util.byteToUnsigned
import com.google.protobuf.ByteString

import scala.collection.mutable

class RecordArray(val buffer: mutable.Buffer[Byte]) extends mutable.IndexedSeq[RecordPtr] {
  override def update(idx: Int, elem: RecordPtr): Unit = update(idx, elem.getIter)

  private def update(idx: Int, elem: IterableOnce[Byte]): Unit = buffer.patchInPlace(idx * BYTE_COUNT_IN_RECORD, elem, BYTE_COUNT_IN_RECORD)

  override def apply(idx: Int): RecordPtr = new RecordPtr(buffer, idx)

  override def length: Int = buffer.length / BYTE_COUNT_IN_RECORD

  def toByteString: ByteString = {
    val iter = buffer.iterator
    ByteString.readFrom(() =>
      if (iter.hasNext) {
        byteToUnsigned(iter.next())
      } else
        -1
    )
  }
}

object RecordArray {
  def from(array: Array[Byte]): RecordArray = from(array.iterator)

  private def from(iter: Iterator[Byte]): RecordArray = {
    val buffer = mutable.ArrayBuffer.fill[Byte](iter.length)(0)
    buffer.patchInPlace(0, new IterableOnce[Byte] {
      override def iterator: Iterator[Byte] = iter
    }, iter.length)
    new RecordArray(buffer)
  }
}
