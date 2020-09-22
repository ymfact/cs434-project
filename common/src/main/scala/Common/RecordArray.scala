package Common

import Common.Const.{BYTE_COUNT_IN_KEY, BYTE_COUNT_IN_RECORD, BYTE_OFFSET_OF_KEY}
import Common.Util.byteToUnsigned
import com.google.protobuf.ByteString

import scala.collection.mutable

class RecordArray(val raw: Array[Byte]) {

  def length: Int = raw.length / BYTE_COUNT_IN_RECORD

  def patchInPlace(thisIdx: Int, that: RecordArray, thatIdx: Int, len: Int = 1): Unit =
    System.arraycopy(that.raw, thatIdx * BYTE_COUNT_IN_RECORD, this.raw, thisIdx * BYTE_COUNT_IN_RECORD, len * BYTE_COUNT_IN_RECORD)

  def compareRecord(left: Int, right: Int): Int = {
    val thisKey = Record.getKeyView(raw.view.slice(left * BYTE_COUNT_IN_RECORD + BYTE_OFFSET_OF_KEY, left * BYTE_COUNT_IN_RECORD + BYTE_OFFSET_OF_KEY + BYTE_COUNT_IN_KEY))
    val thatKey = Record.getKeyView(raw.view.slice(right * BYTE_COUNT_IN_RECORD + BYTE_OFFSET_OF_KEY, right * BYTE_COUNT_IN_RECORD + BYTE_OFFSET_OF_KEY + BYTE_COUNT_IN_KEY))
    Record.compare(thisKey, thatKey)
  }

  def toByteString: ByteString = {
    val iter = raw.iterator.map(byteToUnsigned).take(raw.length) ++ Iterator.continually(-1)
    ByteString.readFrom(() => iter.next)
  }
}

object RecordArray {
  def from(array: Array[Byte]): RecordArray = new RecordArray(array)
}
