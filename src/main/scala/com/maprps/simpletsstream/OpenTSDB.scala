package com.maprps.simpletsstream

/**
  * Created by dmeng on 10/16/16.
  */


import com.sun.jersey.api.client.Client
import com.sun.jersey.api.client.ClientResponse

import javax.ws.rs.core.MediaType

import org.json4s.{JsonAST, NoTypeHints}
import org.json4s.JsonDSL._
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.write

object OpenTSDB {

    def toTSDB(iter: Iterator[String]) : Iterator[String] = {

        var failCount = 0 // keep a count of failed records
        var roundRobinIter = 0 // to implement round robin balancer
        var res = List[JsonAST.JObject]()
        implicit val formats = Serialization.formats(NoTypeHints)

        // add or change the addresses of multiple tsdb instances here
        val tsdArray = Array("http://localhost:4242/api/put?details")
        val tsdLength = tsdArray.length
        val restClient = Client.create()

        while (iter.hasNext) {
            val webResource = restClient.resource(tsdArray(roundRobinIter%tsdLength))
            val rclientResponseClazz = classOf[ClientResponse]

            for (putIter <- 0 to 49) {
                if (iter.hasNext) {
                    val cur = iter.next
                    val splitCur = cur.split(" ")
                    val tags = splitCur.slice(3,splitCur.length)
                    val tagsArray = for (x<-tags) yield x.toString.split("=")
                    val tagsTuple = for (x<-tagsArray) yield (x(0),x(1))
                    val json = ("metric" -> splitCur(0)) ~
                        ("timestamp" -> splitCur(1)) ~
                        ("value" -> splitCur(2)) ~
                        ("tags" -> ( tagsTuple.toMap))
                    // val line = cur.map(line => line.split("\t").map(elem => elem.trim))
                    // Check if line has correct # of elements
                    // otherwise flag as invalid and increment accum var
                    res = res :+ json
                }
            }
            var resSerialized = write(res)

            // single put request format will be different
            if (res.length==1){
                resSerialized = resSerialized.stripPrefix("[").stripSuffix("]").trim
            }

            val response = webResource
                .`type`(MediaType.APPLICATION_FORM_URLENCODED_TYPE)
                .post(rclientResponseClazz, resSerialized)

            // printf(resSerialized)
            printf("------------------------------------------------")
            val message = response.getEntity(classOf[String])
            printf(message)
            printf("------------------------------------------------")
            printf(getResponseMessage(response.getStatus().toInt))
            printf("------------------------------------------------")

            failCount += message.substring(message.indexOfSlice("failed")+8, message.indexOfSlice("success") - 2).toInt
            res = List[JsonAST.JObject]()
            roundRobinIter += 1
            response.close()
        }
        restClient.destroy()
        printf("++++++++++++++++++++++++++++++++++++++++++++++++")
        printf("++++++++++++++++++++++++++++++++++++++++++++++++")
        printf(s"the number of records failed is $failCount")
        printf("++++++++++++++++++++++++++++++++++++++++++++++++")
        printf("++++++++++++++++++++++++++++++++++++++++++++++++")
        iter
    }

    def getResponseMessage(code: Int) : String = {
        // provide additional message on the http response
        val code400 = "Information provided by the API user, via a query string or content data, was in error or missing. This will usually include information in the error body about what parameter caused the issue. Correct the data and try again."
        val code404 = "Either the requested annotation does not exist or the requested endpoint or file was not found. This is usually related to the static file endpoint."
        val code405 = "The requested verb or method was not allowed. Please see the documentation for the endpoint you are attempting to access."
        val code406 = "The request could not generate a response in the format specified. For example, if you ask for a PNG file of the logs endpoing, you will get a 406 response since log entries cannot be converted to a PNG image (easily)"
        val code408 = "The request has timed out. This may be due to a timeout fetching data from the underlying storage system or other issues."
        val code413 = "The results returned from a query may be too large for the server's buffers to handle. This can happen if you request a lot of raw data from OpenTSDB. In such cases break your query up into smaller queries and run each individually"
        val code500 = "An internal error occured within OpenTSDB. Make sure all of the systems OpenTSDB depends on are accessible and check the bug list for issues"
        val code501 = "The requested feature has not been implemented yet. This may appear with formatters or when calling methods that depend on plugins"
        val code503 = "A temporary overload has occurred. Check with other users/applications that are interacting with OpenTSDB and determine if you need to reduce requests or scale your system."
        val code000 = "No such response code"
        val code204 = "Success"
        val message = code match {
            case 400 => code400
            case 404 => code404
            case 405 => code405
            case 406 => code406
            case 408 => code408
            case 413 => code413
            case 500 => code500
            case 501 => code501
            case 503 => code503
            case 204 => code204
            case 200 => code204
            case _ => code000
        }
        message.toString
    }

}
