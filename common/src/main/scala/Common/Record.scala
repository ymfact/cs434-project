package Common

import java.io.{DataInputStream, EOFException, InputStream}

import Common.Const.{BYTE_COUNT_IN_KEY, BYTE_COUNT_IN_RECORD, BYTE_OFFSET_OF_KEY}
import com.google.protobuf.ByteString

trait Record extends Ordered[Record] {

  import Record._

  protected def getKeyIter: Unit => Byte

  override def compare(that: Record): Int = {
    val thisIter = getCompareIter(this.getKeyIter)
    val thatIter = getCompareIter(that.getKeyIter)
    compare(thisIter, thatIter)
  }

  def compare(that: ByteString): Int = {
    val thisIter = getCompareIter(this.getKeyIter)
    val thatIter = getCompareIter(Record.getKeyIter(that))
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
}

object Record {
  def from(that: ByteString): RecordFromByteString = new RecordFromByteString(that)

  def from(stream: DataInputStream): RecordFromStream = new RecordFromStream(Files.readSome(stream, BYTE_COUNT_IN_RECORD))

  private def getCompareIter(iter: Unit => Byte): Iterator[Byte] = new Iterator[Byte] {
    private var nextIndex = BYTE_OFFSET_OF_KEY

    override def hasNext: Boolean = nextIndex < BYTE_COUNT_IN_KEY

    override def next(): Byte = {
      nextIndex += 1
      iter.apply()
    }
  }

  def getKeyIter(iterable: IterableOnce[Byte]): Unit => Byte = {
    val iter = iterable.iterator
    _ => iter.next
  }

  def getKeyIter(byteString: ByteString): Unit => Byte = {
    val iter = byteString.iterator
    _ => iter.next
  }
}