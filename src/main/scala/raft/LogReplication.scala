package raft

import akka.actor._
import Protocol._
import raft.util.persistence._
import java.util.concurrent.atomic._

/**
 * asyc log replication
 * T is the type of "event", which typically is a case class with type T representing the command from client
 */
//class LogReplication (leader : ActorRef,  followers : Seq[ActorRef], dbPath : String) extends Actor with ActorLogging {  
class LogReplication extends Actor {
  
 private val dbPath = "/Users/morefree/Developments/scala-workspace/some" + LogReplication.getDbIndex
private val db = new LevelDB(dbPath)
 println(dbPath)
  
	def receive = {
	  case e : AppendEntries => 
	    println("id = " + e.leaderId)
	    asyncWrite(e)
	}
	
	
	def asyncWrite(e : AppendEntries) = {
		db.open
		try {
		  println(e.entry.getClass)
		  db.put(e.term.toString, POJO.serialize(e.entry.asInstanceOf[Object]))
		} finally {
		  db.close
		}
	}
}

object LogReplication {
	var dbIndex : AtomicInteger = new AtomicInteger(1)
	def getDbIndex = dbIndex.getAndIncrement()
}