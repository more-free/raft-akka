package raft.util.persistence

import akka.actor.{ActorLogging, Actor, ActorRef}
import raft.Protocol._
import scala.collection.mutable.Map

/**
 * Created by kexu1 on 8/27/14.
 */
class LogKeeper (leaderLogRep : ActorRef, followerLogRep : ActorRef, db : LogManager) extends Actor with ActorLogging {
  private var pendingEntries : Map[Int, AppendEntries] = _

  private def earliestEntry : Option[AppendEntries] = {
    if(pendingEntries.empty) None
    else Some(pendingEntries.get(pendingEntries.keys.min))
  }

  override def receive = {
    // from LeaderLogRep.  forward to followerLogRep, and make sure it's received
    case entry : AppendEntries =>
      pendingEntries += ( entry.prevLogIndex -> entry )
      followerLogRep ! entry

    // from followerLogRep. re-try if not success
    case response : AppendResult =>
      // failure => refused by the followerLogRep due to inconsistency between leader and follower
      // for this case, the leader needs to continue sending previous entry to follower, until follower
      // accepts that entry (say it's e). After that, the leader needs to re-send all entries after e
      // to the follower. Details are described on the Raft paper page 7.
      if ( ! response.success ) {
        followerLogRep ! db.get( (response.prevLogIndex - 1).toString )  // send the previous entry

      } else {
        // re-send all entries after the last consistent entry
        val lastIndex = Integer.parseInt(db.lastIndex)
        if(lastIndex > response.prevLogIndex + 1)
          followerLogRep ! db.get( (response.prevLogIndex + 1 + 1).toString )
        else {
          // notify leader
          leaderLogRep ! response
          // remove from pending entries
          pendingEntries.remove(response.prevLogIndex)
          // continue to send next pending entry if any
          earliestEntry match {
            case Some(entry) => followerLogRep ! entry
          }
        }
      }

    // from leaderLogRep. forward to followerLogRep
    case commit : CommitLog =>
      followerLogRep ! commit

    // commit result, from followerLogRep
    case result : CommitResult =>
      if(!result.success) {
        leaderLogRep ! result
      } else {
        followerLogRep ! CommitLog(result.prevLogIndex)
      }
  }
}
