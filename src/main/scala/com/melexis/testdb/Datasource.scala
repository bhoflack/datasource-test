package com.melexis.testdb

import com.datastax.driver.core.{Cluster, ConsistencyLevel, Session}
import java.util.UUID

object Datasource {

    def initSchema(session: Session) = {
        session.execute("""
              CREATE TABLE testlog (
                  id uuid,
                  test text,
                  coordinate text,
                  value text,
                  PRIMARY KEY (id, test, coordinate)
              ) WITH COMPACT STORAGE
          """)
    }

    def dropSchema(session: Session) = {
        session.execute("""DROP TABLE testlog""")
    }

    def write(session: Session,
              id: UUID,
              values: Map[String, Map[String, String]]) = {
        val insertStmt = session.
                prepare("""INSERT INTO testlog (id, test, coordinate, value) 
                           VALUES (?, ?, ?, ?)""").
                setConsistencyLevel(ConsistencyLevel.ONE)

        println("Start writing dies")
        
        val now = System.nanoTime

        val futures = for (
            (test, coordinates) <- values;
            (coordinate, value) <- coordinates
        ) yield session.executeAsync(insertStmt.bind(id, test, coordinate, value))

        futures.foreach { future => future.getUninterruptibly }

        val micros = (System.nanoTime - now) / 1000
        println("Wrote the data in %d microseconds.".format(micros))
    }
}
