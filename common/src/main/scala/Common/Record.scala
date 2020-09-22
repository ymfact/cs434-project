package Common

import java.io.DataInputStream

import Common.Const.{BYTE_COUNT_IN_KEY, BYTE_COUNT_IN_RECORD, BYTE_OFFSET_OF_KEY}
import com.google.protobuf.ByteString

trait Record extends Ordered[Record] {
  override def compare(that: Record): Int = {
    val thisIter = this.getKeyIter
    val thatIter = that.getKeyIter
    compare(thisIter, thatIter)
  }

  def compare(that: ByteString): Int = {
    val thisIter = this.getKeyIter
    val thatIter = Record.getKeyIter(that)
    compare(thisIter, thatIter)
  }

  private def compare(iterLeft: Iterator[Byte], iterRight: Iterator[Byte]): Int = {
    for ((left, right) <- iterLeft.zip(iterRight)) {
      val compared = left.compareTo(right)
      if (compared != 0)
        return compared
    }
    0
  }

  protected def getKeyIter: Iterator[Byte]
}

object Record {
  def from(that: ByteString): RecordFromByteString = new RecordFromByteString(that)

  def from(stream: DataInputStream): RecordFromStream = new RecordFromStream(Files.readSome(stream, BYTE_COUNT_IN_RECORD))

  def getKeyIter(iter: Iterator[Byte]): Iterator[Byte] = iter.slice(BYTE_OFFSET_OF_KEY, BYTE_OFFSET_OF_KEY + BYTE_COUNT_IN_KEY)

  def getKeyIter(byteString: ByteString): Iterator[Byte] = new Iterator[Byte] {
    private val iter = byteString.iterator()
    private var nextIndex = BYTE_OFFSET_OF_KEY

    override def hasNext: Boolean = nextIndex < BYTE_COUNT_IN_KEY

    override def next(): Byte = {
      nextIndex += 1
      iter.nextByte()
    }
  }
}