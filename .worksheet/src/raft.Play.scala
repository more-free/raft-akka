package raft

object Play {;import org.scalaide.worksheet.runtime.library.WorksheetSupport._; def main(args: Array[String])=$execute{;$skip(71); 
  println("Welcome to the Scala worksheet");$skip(18); val res$0 = 
  
  1 to 10 size
  
   
  import Role._;System.out.println("""res0: Int = """ + $show(res$0));$skip(52); 
  println(Follower.toString)}
}


object Role extends Enumeration {
    type Role = Value
    val Follower, Candidate, Leader = Value
  }
  