import hello.Hello
import org.apache.logging.log4j.scala.Logging
import scalaj.http.Http
import scalapb.GeneratedMessage

object Main extends App with Logging {

  def send(data: GeneratedMessage): Array[Byte] ={
    Http("http://127.0.0.1:65400")
      .postData(data.toByteArray)
      .timeout(60*60*1000, 60*60*1000)
      .asBytes
      .body
  }

  logger.info(s"Initialized")

  while(true){
    val response = send(new Hello("Master"))
    val hello = Hello.parseFrom(response)
    logger.info(s"Hello from ${hello.from}")
    Thread.sleep(3000)
  }
}