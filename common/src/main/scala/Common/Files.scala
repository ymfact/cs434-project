package Common

import java.io.{BufferedInputStream, BufferedOutputStream, DataInputStream, DataOutputStream}
import java.nio.file.Files.{newInputStream, newOutputStream, readAllBytes}
import java.nio.file.Path

import Common.Const.FILE_CHUNK_SIE
import com.google.protobuf.ByteString
import org.apache.logging.log4j.scala.Logging

import scala.util.Using

object Files extends Logging {

  def inputStream[T](path: Path)(f: DataInputStream => T): T = Using(inputStreamShouldBeClosed(path))(f).get

  def inputStreamShouldBeClosed(path: Path) = new DataInputStream(new BufferedInputStream(newInputStream(path), FILE_CHUNK_SIE))

  def readSome(stream: DataInputStream, len: Int): Array[Byte] = {
    val buffer = Array.ofDim[Byte](len)
    stream.readFully(buffer)
    buffer
  }

  def readAll(path: Path): Array[Byte] = readAllBytes(path)

  def write(path: Path, data: ByteString): Unit = outputStream(path)(data.writeTo)

  def outputStream[T](path: Path)(f: DataOutputStream => T): T = Using(outputStreamShouldBeClosed(path))(f).get

  def outputStreamShouldBeClosed(path: Path) = new DataOutputStream(new BufferedOutputStream(newOutputStream(path), FILE_CHUNK_SIE))

  def write(path: Path, records: Iterable[RecordFromStream]): Unit = {
    outputStream(path) { stream =>
      for (record <- records)
        stream.write(record.raw)
    }
  }
}
