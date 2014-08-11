package raft.util.persistence

import java.io._

object POJO {
	def serialize(obj : Object) = {
		val out = new ByteArrayOutputStream();
		val os = new ObjectOutputStream(out);
		os.writeObject(obj);
		out.toByteArray();
	}
	
	def deserialize(data : Array[Byte]) = {
		val in = new ByteArrayInputStream(data);
		val is = new ObjectInputStream(in);
		is.readObject();
	}
}