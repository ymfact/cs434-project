package Common

import Common.Const.BYTE_COUNT_IN_RECORD
import Common.Util.byteToUnsigned
import com.google.protobuf.ByteString

import scala.collection.mutable

class RecordArray(val buffer: mutable.Buffer[Byte]) extends mutable.IndexedSeq[RecordPtr] {
  override def update(idx: Int, elem: RecordPtr): Unit = buffer.patchInPlace(idx * BYTE_COUNT_IN_RECORD, elem.getView, BYTE_COUNT_IN_RECORD)

  override def apply(idx: Int): RecordPtr = new RecordPtr(buffer, idx)

  override def length: Int = buffer.length / BYTE_COUNT_IN_RECORD

  def toByteString: ByteString = {
    val iter = buffer.iterator.map(byteToUnsigned).take(buffer.length) ++ Iterator.continually(-1)
    ByteString.readFrom(() => iter.next)
  }
}

object RecordArray {
  def from(array: Array[Byte]): RecordArray = {
    new RecordArray(mutable.ArrayBuffer.from(array))
  }
}
