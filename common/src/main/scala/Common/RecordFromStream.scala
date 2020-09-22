package Common

import Common.Const.{BYTE_COUNT_IN_KEY, BYTE_OFFSET_OF_KEY}
import com.google.protobuf.ByteString

import scala.collection.View

class RecordFromStream(val raw: Array[Byte]) extends Record {
  override def getKeyView: View[Byte] = Record.getKeyView(raw.view)

  def copyKey: ByteString = ByteString.copyFrom(raw, BYTE_OFFSET_OF_KEY, BYTE_COUNT_IN_KEY)
}
