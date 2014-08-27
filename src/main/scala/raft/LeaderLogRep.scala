package raft

import akka.actor._
import Protocol._
import raft.util.persistence._

class LeaderLogRep (followerLogReps : Seq[ActorRef], dbPath : String) extends Actor with ActorLogging {
	private val db = new LogManager(dbPath)
	private var lastIndex = db.lastIndex  
	
	// for each logIndex, record AppendEntry confirmations it received from followers
	
	def receive = {
	  // from clients (maybe indirectly from clients : followers forward client requests to leader)
	  case e : AppendEntries => 
	    // increment log index and write to leader's log
	    lastIndex += 1
	    db.put(lastIndex, Entry(e.term, e.entry))
	    
	    // send AppendEntries to all followers until each of them succeeds, 
	    // but only wait for confirmation from the majority before it sends CommitLog 
	    // to all followers
	    followerLogReps.foreach(_ ! e)
	    
	}
}