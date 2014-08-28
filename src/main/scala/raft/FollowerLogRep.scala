package raft

import Protocol._
import util.persistence.LogManager

class FollowerLogRep (dbPath : String) {
  private val db = new LogManager(dbPath)

  private var maxIndex = db.maxIndex
  var term : Int = _  // will be changed outside by the main follower actor

  private def isConsistent(entry : AppendEntries) : Boolean = {
    // it's the first entry, then accept it no matter what term it is
    if(entry.prevLogIndex == -1) true 

    val data = db.get(entry.prevLogIndex)
    data != null && data.asInstanceOf[Entry].term == entry.prevLogTerm
  }

  def replicateLog(entry : AppendEntries) : Boolean = {
    if(!isConsistent(entry)) false

    maxIndex += 1
    db.put(maxIndex, Entry(entry.term, entry.entry))
    true
  }
  
  /** replicate unconditionally */
  def replicateLogNow(entry : Entry) = {
    maxIndex += 1
    db.put(maxIndex, entry)
  }
}