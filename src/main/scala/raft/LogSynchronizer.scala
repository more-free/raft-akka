package raft

import akka.actor.{ActorLogging, Actor, ActorRef}
import Protocol._
import util.persistence.LogManager
import scala.collection.mutable.Map

/**
 * Created by kexu1 on 8/27/14.
 */
class LogSynchronizer (leaderLogRep : ActorRef, follower : ActorRef, db : LogManager) extends Actor with ActorLogging {
  private val pendingEntries : Map[Int, AppendEntries] = Map()

  private def earliestEntry : Option[AppendEntries] = {
    if(pendingEntries.isEmpty) None
    else pendingEntries.get(pendingEntries.keys.min)
  }

  override def receive = {
    // from LeaderLogRep.  forward to followerLogRep, and make sure it's received
    case entry : AppendEntries =>
      pendingEntries += ( entry.prevLogIndex -> entry )
      if(pendingEntries.size == 1)
    	  follower ! entry

    // from followerLogRep. re-try if not success
    case response : AppendResult =>
      // failure => refused by the followerLogRep due to inconsistency between leader and follower
      // for this case, the leader needs to continue sending previous entry to follower, until follower
      // accepts that entry (say it's e). After that, the leader needs to re-send all entries after e
      // to the follower. Details are described on the Raft paper page 7.
      if ( ! response.success ) {
        // send the previous entry. follower must accept this entry unconditionally
        follower ! db.get( response.prevLogIndex - 1).asInstanceOf[Entry]

      } else {
        // re-send all entries after the last consistent entry. follower must accept these entries unconditionally
        if(db.maxIndex > response.prevLogIndex + 1) {
          follower ! db.get( response.prevLogIndex + 1 + 1 ).asInstanceOf[Entry]
        } else {
          // notify leader
          leaderLogRep ! response
          // remove from pending entries
          pendingEntries.remove(response.prevLogIndex)
          // continue to send next pending entry if any
          earliestEntry match {
            case Some(entry) => follower ! entry
            case None => 
          }
        }
      }

    // from leaderLogRep. forward to followerLogRep
    case commit : CommitLog =>
      follower ! commit

    // commit result, from followerLogRep
    case result : CommitResult =>
      if(!result.success) {
        leaderLogRep ! result
      } else {
        follower ! CommitLog(result.prevLogIndex)
      }
  }
}
