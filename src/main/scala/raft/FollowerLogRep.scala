package raft

import Protocol._
import util.persistence.LogManager

class FollowerLogRep (db : LogManager) {
  private var maxIndex = db.maxIndex // init to db.maxIndex instead of -1 in case restart
  var term : Int = _  // will be changed outside by the main follower actor

  def replicateLog(e : AppendEntries) : AppendResult = {
    // it's the first entry, then accept it no matter what term it is
    if(e.prevLogIndex == -1) {
      append(e)
    }

    // or if it's not the first entry, but it is consistent with the follower's log
    val lastLogIndex = db.maxIndex
    val lastLogEntry = db.get(lastLogIndex).asInstanceOf[Entry]
    if(lastLogEntry != null && lastLogEntry.term == e.prevLogTerm) {
      append(e)
    }

    // not consistent, reject to replicate
    AppendResult(lastLogIndex, lastLogEntry.term, false)
  }


  private def append(e : AppendEntries) : AppendResult = {
    maxIndex += 1
    db.put(maxIndex, e.entry)
    AppendResult(maxIndex, e.entry.asInstanceOf[Entry].term, true)
  }

  def getLogManager = db
}