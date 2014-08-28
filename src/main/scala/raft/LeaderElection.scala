package raft

import akka.actor._
import akka.actor.Actor.Receive
import scala.concurrent.duration._
import Protocol._

case class Tick()

class LeaderElection extends Actor with ActorLogging {
  import LeaderElection._
  import LeaderElection.Role._

  private val rnd = new java.util.Random();
  private def electionTimeout =
    (rnd.nextInt(electionTimeoutRange.size) + electionTimeoutRange.start) milliseconds

  private var leader : ActorRef = _
  private var role = Follower

  private val others = nodes.filterNot(_ eq self)
  private val id = nodes.indexWhere(_ eq self)
  private val majority = nodes.size / 2 + 1
  private val logger = loggers(id)

  private val dbPath : String = "some path"   // for log replication
  private val followerLogRep : FollowerLogRep = new FollowerLogRep(dbPath)

  /**
   * Follower only responds to requests, never send requests to others.
   * once it does not receive HeartBeat within electionTimeout,
   * it becomes Candidate and start a new election
   * otherwise, it remains follower
   */
  def follower(hasVoted: Boolean, myTerm: Int): Receive = {
    case DoYouCopy(term) =>
      val t = math.max(myTerm, term)
      leader = sender
      sender ! Copy(t)
      context.become(follower(hasVoted, t), true)

    case RequestVote(term, candidateId) =>
      if ((term == myTerm && !hasVoted) || term > myTerm) {
        sender ! Vote(term)
        context.become(follower(true, term), true)
      }

    case ReceiveTimeout =>
      becomeCandidate(myTerm + 1)

    /* messages for log replications */
   
    // appendEntry request from clients. forward to leader
    case entry : AppendEntries =>
    	leader ! entry  // TODO store clients - entry mapping
    
    // entry from leader. accept it unconditionally
    case entry : Entry => 
      	followerLogRep.replicateLogNow(entry)
    
    // TODO commit log and return result to client
    case commit : CommitLog =>
    
  } 
  
  private var index = 1
  
  
  /**
   * A candidate continues in
   * this state until one of three things happens: (a) it wins the
   * election, (b) another server establishes itself as leader, or
   * (c) a period of time goes by with no winner.
   *
   */

  def candidate(myTerm: Int, myVotes: Int): Receive = {
    case Vote(term) =>
      if (term > myTerm)
        becomeFollower(false, term)
      else if (term == myTerm) {
        if (myVotes + 1 >= majority) {
          // case (a), self becomes leader and send heart beat to all other servers
          becomeLeader(term)

        } else {
          context.become(candidate(term, myVotes + 1))
        }
      }

    // case (b), another candidate becomes leader
    case DoYouCopy(term) =>
      if (term >= myTerm) {
        // recognize other leader's authority, and revert to follower
        sender ! Copy(term)
        becomeFollower(true, term)
      }

    // case (c), split votes ( no winner in this term, start next term ) 
    case ReceiveTimeout =>
      becomeCandidate(myTerm + 1)

    // in case any network delay / partition, or other failure
    case RequestVote(term, candidateId) =>
      checkStaleCandidate(term, myTerm)
  }

  def leader(myTerm: Int, heartBeatSchr: Cancellable): Receive = {
    case Copy(term) =>
      checkStaleLeader(term, myTerm, heartBeatSchr)
    case RequestVote(term, candidateId) =>
      checkStaleLeader(term, myTerm, heartBeatSchr)
    case DoYouCopy(term) =>
      checkStaleLeader(term, myTerm, heartBeatSchr)
      
    // messages for log replication TODO including count all successfully replicated messages
      
  }

  private def checkStaleLeader(term: Int, myTerm: Int, heartBeatSchr: Cancellable) = {
    if (term > myTerm) {
      heartBeatSchr.cancel
      becomeFollower(false, term)
    }
  }

  private def checkStaleCandidate(term: Int, myTerm: Int) = {
    if (term > myTerm)
      becomeFollower(false, term)
  }

  private def becomeFollower(hasVoted: Boolean, myTerm: Int) = {
    context.become(follower(hasVoted, myTerm), true)
    context.setReceiveTimeout(electionTimeout)

    role = Follower
    log.info(self.path.name + " becomes FOLLOWER at term " + myTerm)
  }

  private def becomeCandidate(myTerm: Int) = {
    // become candidate, increment myTerm and vote for self
    context.become(candidate(myTerm, 1), true)
    // send request for vote to all servers
    others.foreach(_ ! RequestVote(myTerm, id))
    // reset random election timeout
    context.setReceiveTimeout(electionTimeout)

    role = Candidate
    log.info(self.path.name + " becomes CANDIDATE at term " + myTerm)
  }

  private def becomeLeader(myTerm: Int) = {
    context.become(leader(myTerm, scheduleHeartBeat(myTerm)), true)

    role = Leader
    leader = self
    log.info(self.path.name + " becomes LEADER at term " + myTerm)
  }

  private def scheduleHeartBeat(term: Int): Cancellable = {
    import LeaderElection.system.dispatcher
    context.system.scheduler.schedule(0 milliseconds, heartBeatPeriod) {
      others.foreach(_ ! DoYouCopy(term))
    }
  }

  def receive = {
    case Tick =>
      becomeFollower(false, 0)
  }
}

object LeaderElection {
  val heartBeatPeriod = 200 milliseconds
  val electionTimeoutRange = 1000 to 2000
  lazy val system = ActorSystem("raft")
  lazy val nodes = (1 to 5 toList) map ("node-" + _) map (n => system.actorOf(Props[LeaderElection], name = n))
  lazy val loggers = (1 to 5 toList) map ("logger-" + _) map (n => system.actorOf(Props[LogReplication], name = n))
  
  object Role extends Enumeration {
    type Role = Value
    val Follower, Candidate, Leader = Value
  }

  /**
   * kick off leader election
   */
  def start = nodes.foreach(_ ! Tick)

  /**
   * shut down actor system
   */
  def stop = system.shutdown
}
  
  