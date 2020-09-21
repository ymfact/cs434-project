package Common

import Common.Const.{BYTE_COUNT_IN_KEY, BYTE_COUNT_IN_RECORD, BYTE_OFFSET_OF_KEY}
import com.google.protobuf.ByteString

import scala.collection.mutable

class RecordPtr(val slicedBuffer: mutable.Buffer[Byte]) extends Record {
  override def getKeyIter: Unit => Byte = Record.getKeyIter(raw)

  def raw: mutable.Buffer[Byte] = slicedBuffer
}

class RecordFromByteString (raw: ByteString) extends Record {
  override def getKeyIter: Unit => Byte = Record.getKeyIter(raw)
}

class RecordFromStream(arr: Array[Byte]) extends Record {
  override def getKeyIter: Unit => Byte = Record.getKeyIter(raw)

  def raw: Array[Byte] = arr

  def key: Array[Byte] = arr.slice(BYTE_OFFSET_OF_KEY, BYTE_OFFSET_OF_KEY + BYTE_COUNT_IN_KEY)
}
