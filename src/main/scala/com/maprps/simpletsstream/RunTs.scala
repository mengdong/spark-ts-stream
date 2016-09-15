package com.maprps.simpletsstream

import scopt.OptionParser
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object RunTs extends Serializable {

    case class runParams(
                          param1: String = null
                        )

    def main(args: Array[String]): Unit = {
        Logger.getLogger("org").setLevel(Level.WARN)
        Logger.getLogger("akka").setLevel(Level.WARN)

        val conf = new SparkConf().setAppName(this.getClass.getSimpleName)
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        // conf.registerKryoClasses(Array(classOf[class1], classOf[class2]))
        val sc = new SparkContext(conf)

        val defaultParam = new runParams()
        val parser = new OptionParser[runParams](this.getClass.getSimpleName) {
            head(s"${this.getClass.getSimpleName}: Run simple app")
            opt[String]("param1")
              .text("sample_param1")
              .action((x, c) => c.copy(param1 = x))
              .required()
        }
        parser.parse(args, defaultParam).map { params =>
            run(sc, params.param1)
        } getOrElse {
            sys.exit(1)
        }
        // sc.stop()
    }

    def run(sc: SparkContext, param1: String): Unit = {
        // val param1 = "sample content of param1"
        // got SparkContext, just do something
    }
}

        
