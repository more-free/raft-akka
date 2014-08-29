package raft

import akka.actor.{ActorLogging, Actor, ActorRef}
import Protocol._
import util.persistence.LogManager
import scala.collection.mutable.Map

/**
 * Created by kexu1 on 8/27/14.
 * Owned by Leader Actor
 */
class LogSynchronizer (leaderLogRep : ActorRef, follower : ActorRef, db : LogManager) extends Actor with ActorLogging {
  /** key = logIndex (which is prevLogIndex + 1), value = entry.
    * all pending entries have already been written to DB. */
  private val pendingEntries = Map[Int, AppendEntries]()

  private def earliestEntry : Option[AppendEntries] = {
    if(pendingEntries.isEmpty) None
    else pendingEntries.get(pendingEntries.keys.min)
  }

  override def receive = {
    // from LeaderLogRep.  forward to followerLogRep, and make sure it's received
    case entry : AppendEntries =>
      pendingEntries += ( entry.prevLogIndex + 1 -> entry )
      if(pendingEntries.size == 1)
    	  follower ! entry

    // from followerLogRep. re-try if not success
    case response : AppendResult =>
      // failure => refused by the followerLogRep due to inconsistency between leader and follower
      // for this case, the leader needs to continue sending previous entry to follower, until follower
      // accepts that entry (say it's e). After that, the leader needs to re-send all entries after e
      // to the follower. Details are described on the Raft paper page 7.
      if ( ! response.success ) {
        val prevEntry = db.get(response.lastLogIndex).asInstanceOf[Entry]
        if(prevEntry.term == response.lastLogTerm) {
          val nextEntry = db.get(response.lastLogIndex + 1).asInstanceOf[Entry]
          follower ! AppendEntries(response.lastLogIndex, response.lastLogTerm, nextEntry.entry)
        } else {
          // send the previous entry, and hopefully the follower could accept it
          follower ! AppendEntries(response.lastLogIndex - 1,
                                   db.get(response.lastLogIndex - 1).asInstanceOf[Entry].term,
                                   prevEntry.entry)
        }

      } else {
        // may not exist (due to node failure)
        pendingEntries.remove(response.lastLogIndex)

        // re-send all entries after the last consistent entry.
        val nextLogIndex = response.lastLogIndex + 1
        if( !pendingEntries.contains(nextLogIndex) ) {
          val nextEntry = db.get(nextLogIndex).asInstanceOf[Entry]
          follower ! AppendEntries(response.lastLogIndex, response.lastLogTerm, nextEntry)
        } else {
          // normal case, no failure happened
          // notify leader
          leaderLogRep ! response
          // continue to send next pending entry if any
          earliestEntry match {
            case Some(entry) => follower ! entry  // AppendEntries
            case None => 
          }
        }
      }

    // from leaderLogRep. forward to followerLogRep
    case commit : CommitLog =>
      follower ! commit

    // commit result, from follower
    case result : CommitResult =>
      if(!result.success) {
        sender() ! CommitLog(result.lastLogIndex) // re-try
      } else {
        leaderLogRep ! result
      }
  }
}
