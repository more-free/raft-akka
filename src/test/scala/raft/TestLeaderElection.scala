package raft

import org.junit._
import java.util.concurrent._
import raft.util.persistence._

class TestLeaderElection {
  //@Test
  def testLeader = {
    println("Hello, World")
    LeaderElection.start
    TimeUnit.SECONDS.sleep(15)
    LeaderElection.stop
  }
  
  @Test
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
}