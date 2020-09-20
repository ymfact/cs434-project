package Common

import java.io.{DataInputStream, DataOutputStream}
import java.nio.file.Files.{newInputStream, newOutputStream, readAllBytes}
import java.nio.file.Path

import Common.RecordStream.recordsToByteString
import com.google.protobuf.ByteString
import org.apache.logging.log4j.scala.Logging

object Files extends Logging {

  def inputStreamShouldBeClosed(path: Path) = new DataInputStream(newInputStream(path))
  def outputStreamShouldBeClosed(path: Path) = new DataOutputStream(newOutputStream(path))

  def inputStream[T](path: Path)(f: DataInputStream => T): T = {
    val stream = inputStreamShouldBeClosed(path)
    try {
      f(stream)
    } finally {
      stream.close()
    }
  }

  def outputStream[T](path: Path)(f: DataOutputStream => T): T = {
    val stream = outputStreamShouldBeClosed(path)
    try {
      f(stream)
    } finally {
      stream.close()
    }
  }

  def readSome(stream: DataInputStream, len: Int): Array[Byte] ={
    val buffer = Array.ofDim[Byte](len)
    stream.readFully(buffer)
    buffer
  }

  def readAll(path: Path): Array[Byte] = readAllBytes(path)

  def write(path: Path, data: ByteString): Unit = outputStream(path)(data.writeTo)

  def write(path: Path, records: Iterable[RecordFromByteArray]): Unit = write(path, recordsToByteString(records))
}
