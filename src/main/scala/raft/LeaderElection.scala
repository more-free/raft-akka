package raft

import akka.actor._
import akka.actor.Actor.Receive
import scala.concurrent.duration._


class LeaderElection {
	println("something")
}

// heart beat
case class DoYouCopy(term : Int)
case class Copy(term : Int)

// append log
case class AppendEntries(term : Int, leaderId : Int, data : String) 

// vote
case class RequestVote(term : Int, candidateId : Int)  // request voting for candidateId
case class Vote(term : Int)  // vote for candidateId

case class Tick()

class Node extends Actor with ActorLogging {
  import Node._
  import Node.Role._
  
  private val rnd = new java.util.Random();
  private def electionTimeout = 
    (rnd.nextInt(electionTimeoutRange.size) + electionTimeoutRange.start ) milliseconds 
 
  private val others = nodes.filterNot(_ eq self)
  private val id = nodes.indexWhere(_ eq self)
  private val majority = nodes.size / 2 + 1
  
  private var role = Follower 
 
  /**
   * Follower only responds to requests, never send requests to others.
   * once it does not receive HeartBeat within electionTimeout, 
   * it becomes Candidate and start a new election
   * otherwise, it remains follower
   */
  def follower(hasVoted : Boolean, myTerm : Int) : Receive = {
    case DoYouCopy(term) => 
      val t = math.max(myTerm, term)
      sender ! Copy(t)
      context.become(follower(hasVoted, t), true)
        
    case RequestVote(term, candidateId) => 
      if( (term == myTerm && !hasVoted) || term > myTerm ) {
        sender ! Vote(term)
        context.become(follower(true, term), true)
      }
      
    case ReceiveTimeout => 
      println(self.path.name + " timeout at " + myTerm)
      becomeCandidate(myTerm + 1)
  }
  
  /**
   * A candidate continues in
   * this state until one of three things happens: (a) it wins the
   * election, (b) another server establishes itself as leader, or
   * (c) a period of time goes by with no winner.
   * 
   */
  
  def candidate(myTerm : Int, myVotes : Int) : Receive = {
    case Vote(term) => 
      if(term > myTerm) 
        becomeFollower(false, term)
      else if(term == myTerm) {
    	if(myVotes + 1 >= majority) {
    	  // case (a), self becomes leader and send heart beat to all other servers
          becomeLeader(term)
          
        } else {
          context.become(candidate(term, myVotes + 1))
        }
      }
    
    // case (b), another candidate becomes leader
    case DoYouCopy(term) =>
      if(term >= myTerm) {
        // recognize other leader's authority, and revert to follower
        sender ! Copy(term)
        becomeFollower(true, term)
      }
       
    // case (c), split votes ( no winner in this term, start next term ) 
    case ReceiveTimeout => 
      becomeCandidate(myTerm + 1)
    
    // in case any network delay / partition, or other failure
    case RequestVote(term, candidateId) =>
      if(term > myTerm)
        becomeFollower(false, term)
  }
    
  def leader(myTerm : Int, heartBeatSchr : Cancellable) : Receive = {
    case Copy(term) => 
      if(term > myTerm) {
        heartBeatSchr.cancel
        
        println(self.path.name + " reverts to follower")
        becomeFollower(false, term)
      }
      
      println("leader " + self.path.name + " failed !")
      heartBeatSchr.cancel
      
    
    // in case any failure happened
    case RequestVote(term, candidateId) => 
      if(term > myTerm) becomeFollower(false, term)
    case DoYouCopy(term) => 
      if(term > myTerm) becomeFollower(false, term)
  }
  
  private def becomeFollower(hasVoted : Boolean, myTerm : Int) = {
    context.become(follower(hasVoted, myTerm), true)
    context.setReceiveTimeout(electionTimeout)
    
    role = Follower
  }
  
  private def becomeCandidate(myTerm : Int) = {
     // become candidate, increment myTerm and vote for self
      context.become(candidate(myTerm, 1), true) 
      // send request for vote to all servers
      others.foreach(_ ! RequestVote(myTerm, id))
      // reset random election timeout
      context.setReceiveTimeout(electionTimeout) 
      
      role = Candidate
  }
  
  private def becomeLeader(myTerm : Int) = {
	  context.become(leader(myTerm, scheduleHeartBeat(myTerm)), true)
      role = Leader
      println(self.path.name + " becomes leader at term " + myTerm + "!")
  }
  
  private def scheduleHeartBeat(term : Int) : Cancellable = {
	  import Node.system.dispatcher 
	  context.system.scheduler.schedule(0 milliseconds, heartBeatPeriod) {
	    others.foreach(_ ! DoYouCopy(term))
	  }
  }
    
  def receive = {
    case Tick => 
      	println(self.path.name + " becoming follower")
    	becomeFollower(false, 0)
    case e => 
      println("unknown msg type " + e.getClass)
  }
}

object Node {
  val heartBeatPeriod = 200 milliseconds
  val electionTimeoutRange = 1000 to 2000
  val system = ActorSystem("raft")
  val nodes = (1 to 5 toList) map ("node-" + _) map (n => system.actorOf(Props[Node], name = n))
  
  object Role extends Enumeration {
    type Role = Value
    val Follower, Candidate, Leader = Value
  }
  
  // tick off
  def start = nodes.foreach(_ ! Tick)  
  def stop = system.shutdown
}