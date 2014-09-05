package raft.util.persistence

import org.iq80.leveldb._
import org.iq80.leveldb.impl.Iq80DBFactory._
import java.io._

import scala.collection.mutable.ListBuffer

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

  def keySet : Seq[String] = {
    var list = ListBuffer[String]()
    val iter = db.iterator
    try {
      iter.seekToFirst
      while(iter.hasNext) {
        list += asString(iter.peekNext().getKey)
        iter.next
      }
    } finally {
      iter.close()
    }

    list
  }

  def entrySet : Seq[(String, Array[Byte])] = {
    var list = ListBuffer[(String, Array[Byte])]()
    val iter = db.iterator
    try {
      iter.seekToFirst
      while(iter.hasNext) {
        val next = iter.peekNext()
        list += ((asString(next.getKey), next.getValue))
        iter.next()
      }
    } finally {
      iter.close()
    }

    list
  }
	
	/** return -1 if log is empty */
	def maxKey = {
	  val iter = db.iterator
	  var mk = "-1"
	  try {
		iter.seekToFirst
	  	while(iter.hasNext) {
		    var key = asString(iter.peekNext().getKey())
	    	if(key.toInt > mk.toInt) mk = key
	    	iter.next
	  	}
	  } finally {
	    iter.close
	  }
	  mk
	}
}