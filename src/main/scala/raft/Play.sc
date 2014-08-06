package raft

object Play {
  println("Welcome to the Scala worksheet")       //> Welcome to the Scala worksheet
  
  1 to 10 size                                    //> res0: Int = 10
  
   
  import Role._
  println(Follower.toString)                      //> Follower
}


object Role extends Enumeration {
    type Role = Value
    val Follower, Candidate, Leader = Value
  }
  