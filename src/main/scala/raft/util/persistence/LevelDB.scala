package raft.util.persistence

import org.iq80.leveldb._
import org.iq80.leveldb.impl.Iq80DBFactory._
import java.io._

class LevelDB (dbPath : String) {
	private var db : DB = _
  
	def open = {
		val options = new Options
		options.createIfMissing(true)
		db = factory.open(new File(dbPath), options)
	}
	
	def close = db.close
	def put(key : String, value : Array[Byte]) = db.put(bytes(key), value)
	def get(key : String) = db.get(bytes(key))
}