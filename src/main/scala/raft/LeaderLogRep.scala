package raft

import akka.actor._
import Protocol._
import raft.util.persistence._
import scala.collection.mutable.Map
import scala.collection.mutable.Set

class LeaderLogRep (val followers : Seq[ActorRef], val db : LogManager) extends Actor with ActorLogging {

  private lazy val logSynchronizers = followers.map( s => context.actorOf(Props(new LogSynchronizer(self, s, db))))
  // key = logIndex, value = number of successful replication
  private val replicated = Map[Int, Int]()
  // key = logIndex, value = logSynchronizers with outgoing commitLog
  private val committed = Map[Int, Set[ActorRef]]()
  private val majority = followers.size / 2 + 1

	override def receive = {
	  // from clients (maybe indirectly from clients : followers forward client requests to leader)
	  case request : LeaderRequest =>
	    // increment log index and write to leader's log
	    val logIndex = db.lastKey + 1
	    db.put(logIndex, Entry(request.term, request.clientRequest.entry))
      // add to map
      replicated += (logIndex -> 0)
	    
	    // send AppendEntries to all followers until each of them succeeds, 
	    // but only wait for confirmation from the majority before it sends CommitLog 
	    // to all followers
      val prevEntry = db.get(logIndex - 1)
      val appendEntry =
        if(prevEntry == null) AppendEntries(request.term, -1, -1, request.clientRequest.entry)
        else AppendEntries(request.term, logIndex - 1, prevEntry.asInstanceOf[Entry].term, request.clientRequest.entry)

      logSynchronizers.foreach( s =>  s ! appendEntry)


    // result from logSynchronizer (not from follower)
    // wait for majority of followers sending result (success)
    case result : AppendResult =>  // only successful result will be returned
      val newCount = replicated.getOrElse(result.lastLogIndex, 0) + 1 // increment by one
      replicated += (result.lastLogIndex -> newCount)   // update map

      if(newCount >= majority) {
        committed.get(result.lastLogIndex) match {
          case None =>  // already sent the majority. now only need to send the new coming one
            sender() ! CommitLog(result.lastLogIndex)
          case Some(set) =>  // send all (including the leader) and remove from map
            set.foreach( s => s ! CommitLog(result.lastLogIndex) )
            committed.remove(result.lastLogIndex)
        }

        if(newCount == followers.size) { // already sent all
          // remove from map
          replicated.remove(result.lastLogIndex)
        }
      } else {
        // add logSynchronizer in charge to committed map
        val newSet = committed.getOrElse(result.lastLogIndex, Set[ActorRef](context.parent)) // including leader
        newSet += sender() // add sender to set, sender is logSynchronizer actor
        committed += (result.lastLogIndex -> newSet) // update map
      }

    // from LogSynchronizer, indicating successful committed log (if it fails, then
    // LogSynchronizer will retry until it succeeds)
    // note it does not include CommitResult for leader. because CommitLog is sent to Leader actor directly
    // (instead of sending to this LeaderLogRep)
    case commitResult : CommitResult =>
      context.parent ! commitResult  // forward to leader (parent of LeaderLopRep actor)

    // case response : FollowerResponse =>
	}
}