package raft

import raft.Protocol._
import raft.util.persistence.LogManager

class FollowerLogRep (dbPath : String) {
  private val db = new LogManager(dbPath)
  db.keyCmp = (x : String, y : String) => Integer.parseInt(x).compareTo(Integer.parseInt(y))

  private var lastIndex = db.lastIndex
  var term : Int = _  // will be changed outside by the main follower actor

  private def isConsistent(entry : AppendEntries) : Boolean = {
    if(entry.prevLogIndex == -1) True // it's the first entry

    val data = db.get(entry.prevLogIndex)
    data != null && data.asInstanceOf[Entry].term == entry.term
  }

  def replicateLog(entry : AppendEntries) : Boolean = {
    if(!isConsistent(entry)) false

    // TODO add more checking for the case of leader failure
    lastIndex += 1
    db.put(lastIndex, Entry(entry.term, entry.entry))
    true
  }
}