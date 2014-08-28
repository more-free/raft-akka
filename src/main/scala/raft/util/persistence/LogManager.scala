package raft.util.persistence

import POJO._

// TODO  write every method in a non-blocking fashion
/** key = prevLogIndex , value = Entry(term, entry) */
class LogManager (dbPath : String) {
	private val db : LevelDB = new LevelDB(dbPath)
	db.open
		
	private var lastKey : Int = _
	private var lastValue : AnyRef = _

	private var maxKey : Int = -1
	
	def put(key : Int, value : AnyRef) = {
	  db.put(key.toString, serialize(value))
	  lastKey = key
	  lastValue = value

	  updateMaxKey(key)
	}

	private def updateMaxKey(key : Int) =  { 
	  maxKey = Math.max(maxKey, key)
	}
	
	def putIfTrue(key : Int, value : AnyRef, condition : () => Boolean) = {
	  if(condition()) put(key, value)
	}
	
	/**
	 * expensive operation. used only when recovering from failure
	 */
	// def lastIndex = db.maxKey

	/** not expensive */
	def maxIndex = maxKey

	def get(key : Int) = deserialize(db.get(key.toString))
	
	def close = db.close
}