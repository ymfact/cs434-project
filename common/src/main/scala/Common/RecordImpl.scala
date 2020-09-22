package Common

import Common.Const.{BYTE_COUNT_IN_KEY, BYTE_COUNT_IN_RECORD, BYTE_OFFSET_OF_KEY}
import com.google.protobuf.ByteString

import scala.collection.mutable

class RecordPtr(buffer: mutable.Buffer[Byte], index: Int) extends Record {
  override def getKeyIter: Iterator[Byte] = Record.getKeyIter(getIter)

  def getIter: Iterator[Byte] = buffer.iterator.slice(index * BYTE_COUNT_IN_RECORD, (index + 1) * BYTE_COUNT_IN_RECORD)
}

class RecordFromByteString(raw: ByteString) extends Record {
  override def getKeyIter: Iterator[Byte] = Record.getKeyIter(raw)
}

class RecordFromStream(val raw: Array[Byte]) extends Record {
  override def getKeyIter: Iterator[Byte] = Record.getKeyIter(raw.iterator)

  def copyKey: ByteString = ByteString.copyFrom(raw, BYTE_OFFSET_OF_KEY, BYTE_COUNT_IN_KEY)
}
