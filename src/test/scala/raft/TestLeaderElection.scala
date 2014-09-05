package raft

import org.junit._
import java.util.concurrent._
import raft.util.persistence._
import akka.actor._
import raft._
import raft.Protocol.{Entry, ClientRequest}

class TestLeaderElection {
  //@Test
  def testLeader = {
    val dbPaths = Array(
      "C:\\log\\raft\\some1",
      "C:\\log\\raft\\some2",
      "C:\\log\\raft\\some3",
      "C:\\log\\raft\\some4",
      "C:\\log\\raft\\some5"
    )

    LeaderElection.nodes =
      (1 to 5 toList) map (n => LeaderElection.system.actorOf(Props(new LeaderElection(dbPaths(n - 1))), name = n.toString))

    LeaderElection.start
    TimeUnit.SECONDS.sleep(15)
    LeaderElection.stop
  }


  //@Test
  def testSerializeCaseClassesWithPojoAndLevelDB = {
    val dbPath = "/Users/morefree/Developments/scala-workspace/some4"
    val db = new LevelDB(dbPath)
    db.open
    
    import Protocol._
    try {
       val bytes = db.get(1.toString())
       val obj = POJO.deserialize(bytes)
       
       println(obj.getClass())
       val t = obj.asInstanceOf[RequestVote]
       println("term = " + t.term)
       println("candidateId = " + t.candidateId)
       
       obj match {
         case RequestVote(t, c) => println("t = " + t + " , c = " + c)
         case _ => println("other")
       }
       
    } finally {
      db.close
    }
  }
  
  //@Test
  def testChillScala = {
    val list = List(1, 3, 6)
    val bytes = SeriDeseri.toBytes(list)
    println("hello," + bytes)
    
    val obj = SeriDeseri.fromBytes(bytes)
    println("world," + obj)
  }
    
  //@Test
  def testScalaPickling = {
   
  }
  
  //@Test
  def testPOJO = {
    val list = List(1, 2, 3)
    val bytes = POJO.serialize(list)
    val obj = POJO.deserialize(bytes)
    println(obj)
  }
  
  //@Test
  def testLevelDB = {
    val db = new LevelDB("/Users/morefree/Developments/scala-workspace/some")
    val list1 = List(1, 2, 3)
    val bytes1 = POJO.serialize(list1)
    
    val list2 = List(5)
    val bytes2 = POJO.serialize(list2)
    
    db.put("l1", bytes1)
    db.put("l2", bytes2)
    
    db.close
    
    // read and check
    val db2 = new LevelDB("/Users/morefree/Developments/scala-workspace/some")
    println(POJO.deserialize(db2.get("l1")))
    println(POJO.deserialize(db2.get("l2")))
    db2.close
  }

  @Test
  def testSomething = {
    println("something happened")
  }

  @Test
  def testLogReplication = {
    // create server-side actors
    val dbPaths = Array(
      "C:\\log\\raft\\some1",
      "C:\\log\\raft\\some2",
      "C:\\log\\raft\\some3",
      "C:\\log\\raft\\some4",
      "C:\\log\\raft\\some5"
    )

    deleteIfExist(dbPaths)

    LeaderElection.nodes =
      (1 to 5 toList) map (n => LeaderElection.system.actorOf(Props(new LeaderElection(dbPaths(n - 1))), name = n.toString))


    // kick off
    LeaderElection.start

    // wait for leader being elected
    TimeUnit.SECONDS.sleep(5)

    // send sample client request
    // NOTE : the log output should contains 5 of each items, i.e., 5 "some command", 5 "more command",
    // etc. , because both leader and followers will receive the command, and followers will forward the command
    // to leader without processing it.
    LeaderElection.nodes.foreach(a => a ! ClientRequest(1, "some command"))
    LeaderElection.nodes.foreach(a => a ! ClientRequest(2, "more command"))
    LeaderElection.nodes.foreach(a => a ! ClientRequest(3, "too more command"))
    LeaderElection.nodes.foreach(a => a ! ClientRequest(4, "yet another command"))

    TimeUnit.SECONDS.sleep(5)

    // print all logs, close DB and shut down
    // by the end of the test, all logs of all actors should be exactly the same
    LeaderElection.clean
    TimeUnit.SECONDS.sleep(5)
    LeaderElection.stop
  }


  private def deleteIfExist(paths : Array[String]) = {
    val files = paths.map(new java.io.File(_)).filter(_.exists()).foreach(deleteFile)
  }

  // java 6 style deletion
  private def deleteFile(file : java.io.File) : Unit = {
    if(file.exists()) {
      if (file.isDirectory)
        file.listFiles().foreach(deleteFile)

      file.delete()
    }
  }
}