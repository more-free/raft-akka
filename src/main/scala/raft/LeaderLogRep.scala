package raft

import akka.actor._
import raft.Protocol._
import raft.util.persistence._

class LeaderLogRep (followers : Seq[ActorRef], dbPath : String) extends Actor with ActorLogging {
	private val db = new LogManager(dbPath)
  db.keyCmp = (x : String, y : String) => Integer.parseInt(x).compareTo(Integer.parseInt(y))

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