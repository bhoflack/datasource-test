package com.melexis.testdb

import com.datastax.driver.core.{Cluster, Session}
import java.util.UUID
import org.scalatest._

class DatasourceSpec extends FlatSpec with Matchers {

    def initKeyspace(cluster: Cluster) = {
        val session = cluster.connect
        session.execute("""
            CREATE KEYSPACE testlog
            WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 }
            """)
    }

    def generateTestlog = {
        val builder = Map.newBuilder[String, Map[String, String]]
        for (test <- 0 to 100) {
            val testBuilder = Map.newBuilder[String, String]
            for (
                x <- 0 to 100;
                y <- 0 to 100) testBuilder += (("%d,%d".format(x,y), "test"))
            builder += ((test.toString, testBuilder.result))
        }

        builder.result
    }

    "A datasource" should "persist a testlog" in {
        println("test")
        val uuid = UUID.randomUUID
        val testlog = generateTestlog

        println("Generated testlog")
        val cluster = Cluster.builder().addContactPoint("192.168.59.103").build
        //initKeyspace(cluster)
        val session = cluster.connect("testlog")

        Datasource.dropSchema(session)
        Datasource.initSchema(session)

        Datasource.write(
            session,
            uuid,
            testlog)
    }
}
