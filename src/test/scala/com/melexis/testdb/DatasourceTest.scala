package com.melexis.testdb

import com.datastax.driver.core.{Cluster, Session}
import java.util.UUID
import org.scalatest._

class DatasourceSpec extends FlatSpec with Matchers {

    var ONE_MEGABYTE : String = _ 
    val arr = new Array[Char](1024)
    java.util.Arrays.fill(arr, 'x')
    ONE_MEGABYTE = new String(arr)

    def initKeyspace(cluster: Cluster) = {
        val session = cluster.connect
//        session.execute("DROP KEYSPACE test_brh")

        session.execute("""
            CREATE KEYSPACE test_brh
            WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', 'sensors' : 3, 'colo': 3 }
            """)
    }

    def generateTestlog = {
        val builder = Map.newBuilder[String, Map[String, String]]
        for (test <- 0 to 1) {
            val testBuilder = Map.newBuilder[String, String]
            for (
                x <- 0 to 1000;
                y <- 0 to 1000) testBuilder += (("%d,%d".format(x,y), ONE_MEGABYTE))
            builder += ((test.toString, testBuilder.result))
        }

        builder.result
    }

    "A datasource" should "persist a testlog" in {
        val uuid = UUID.randomUUID
        val testlog = generateTestlog

        println("Generated testlog")
        val cluster = Cluster.builder().addContactPoint("esb-a-test").build
        initKeyspace(cluster)
        val session = cluster.connect("test_brh")


        Datasource.initSchema(session)

        Datasource.write(
            session,
            uuid,
            testlog)
    }
}
