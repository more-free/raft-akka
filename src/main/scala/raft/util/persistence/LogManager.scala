package raft.util.persistence

import POJO._

// TODO  write every method in a non-blocking fashion
class LogManager (dbPath : String) {
	private val db : LevelDB = new LevelDB(dbPath)
	db.open
		
	var lastKey : String = _
	var lastValue : AnyRef = _
	
	def put(key : String, value : AnyRef) = {
	  db.put(key, serialize(value))
	  lastKey = key
	  lastValue = value
	}
	
	def putIfTrue(key : String, value : AnyRef, condition : () => Boolean) = {
	  if(condition()) put(key, value)
	}
	
	/**
	 * expensive operation. used only when recovering from failure
	 */
	def lastIndex = db.maxKey
}