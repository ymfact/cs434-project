package Common

import Common.Const.{BYTE_COUNT_IN_KEY, BYTE_COUNT_IN_RECORD, BYTE_OFFSET_OF_KEY}
import com.google.protobuf.ByteString

import scala.collection.{View, mutable}

class RecordPtr(buffer: mutable.Buffer[Byte], index: Int) extends Record {
  override def getKeyView: View[Byte] = Record.getKeyView(getView)

  def getView: View[Byte] = buffer.view.slice(index * BYTE_COUNT_IN_RECORD, (index + 1) * BYTE_COUNT_IN_RECORD)
}

class RecordFromByteString(raw: ByteString) extends Record {
  override def getKeyView: View[Byte] = Record.getKeyView(raw)
}

class RecordFromStream(val raw: Array[Byte]) extends Record {
  override def getKeyView: View[Byte] = Record.getKeyView(raw.view)

  def copyKey: ByteString = ByteString.copyFrom(raw, BYTE_OFFSET_OF_KEY, BYTE_COUNT_IN_KEY)
}
