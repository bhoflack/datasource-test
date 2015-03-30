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
        session.execute("""DROP TABLE testlog IF EXISTS""")
    }

    def write(session: Session,
              id: UUID,
              values: Map[String, Map[String, String]]) = {
        val insertStmt = session.
                prepare("""INSERT INTO testlog (id, test, coordinate, value) 
                           VALUES (?, ?, ?, ?)""").
                setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM)

        val now = System.nanoTime

        for (
            (test, coordinates) <- values;
            (coordinate, value) <- coordinates
        ) yield session.executeAsync(insertStmt.bind(id, test, coordinate, value))
    }
}
