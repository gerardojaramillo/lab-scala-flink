/**
 * UsingFlink.scala
 * @author
 *   Gerardo Jaramillo
 */

package lab

import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.connector.file.src.FileSource
import org.apache.flink.connector.file.src.reader.TextLineInputFormat
import org.apache.flink.core.fs.Path
import org.apache.flinkx.api.StreamExecutionEnvironment
import org.apache.flinkx.api.semiauto.stringInfo

import java.time.Duration

object UsingOfFlink {

  def main(args: Array[String]): Unit = {
    require(args.length == 1, "Path argument require.")
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val dir = new Path(args(0))
    
    val fileSource =
      FileSource
        .forRecordStreamFormat[String](new TextLineInputFormat(), dir)
        .monitorContinuously(Duration.ofSeconds(10))
        .build()

    var rawLinesStream =
      env.fromSource(fileSource, WatermarkStrategy.noWatermarks(), "Nice")

    rawLinesStream.print()

    env.execute("UsingOfFlink")
  }

}
