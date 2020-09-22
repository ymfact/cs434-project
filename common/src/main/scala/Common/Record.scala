package Common

import java.io.DataInputStream

import Common.Const.{BYTE_COUNT_IN_KEY, BYTE_COUNT_IN_RECORD, BYTE_OFFSET_OF_KEY}
import com.google.protobuf.ByteString

import scala.collection.View

trait Record extends Ordered[Record] {
  override def compare(that: Record): Int = {
    val thisIter = this.getKeyView
    val thatIter = that.getKeyView
    compare(thisIter, thatIter)
  }

  def compare(that: ByteString): Int = {
    val thisIter = this.getKeyView
    val thatIter = Record.getKeyView(that)
    compare(thisIter, thatIter)
  }

  private def compare(iterLeft: View[Byte], iterRight: View[Byte]): Int = {
    for ((left, right) <- iterLeft.zip(iterRight)) {
      val compared = left.compareTo(right)
      if (compared != 0)
        return compared
    }
    0
  }

  protected def getKeyView: View[Byte]
}

object Record {
  def from(that: ByteString): RecordFromByteString = new RecordFromByteString(that)

  def from(stream: DataInputStream): RecordFromStream = new RecordFromStream(Files.readSome(stream, BYTE_COUNT_IN_RECORD))

  def getKeyView(view: View[Byte]): View[Byte] = view.slice(BYTE_OFFSET_OF_KEY, BYTE_OFFSET_OF_KEY + BYTE_COUNT_IN_KEY)

  def getKeyView(byteString: ByteString): View[Byte] = new View[Byte] {
    override def iterator: Iterator[Byte] = new Iterator[Byte] {
      private val iter = byteString.iterator()
      private var nextIndex = BYTE_OFFSET_OF_KEY

      override def hasNext: Boolean = nextIndex < BYTE_COUNT_IN_KEY

      override def next(): Byte = {
        nextIndex += 1
        iter.nextByte()
      }
    }
  }
}