package raft

import java.util.concurrent.TimeUnit

import akka.actor._
import akka.actor.Actor.Receive
import scala.concurrent.duration._
import Protocol._
import scala.collection.mutable.Map
import raft.util.persistence.LogManager

case class Tick()

class LeaderElection(dbPath : String) extends Actor with ActorLogging {
  import LeaderElection._
  import LeaderElection.Role._

  private val rnd = new java.util.Random();
  private def electionTimeout =
    (rnd.nextInt(electionTimeoutRange.size) + electionTimeoutRange.start) milliseconds

  private var leader : ActorRef = _
  private var role = Follower

  private def others = nodes.filterNot(_ eq self)
  private def id = nodes.indexWhere(_ eq self)
  private def majority = nodes.size / 2 + 1

  private val logManager = new LogManager(dbPath)
  private val followerLogRep : FollowerLogRep = new FollowerLogRep(logManager)

  // key = requestId , value = ActorRef of client
  private val clientRequests = Map[Int, ActorRef]()
  private var leaderLogRepActor : ActorRef = _

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
    case request : ClientRequest =>
      // if leader has not been elected, then tell client to retry
      if(leader == null)
        sender() ! Retry
      else {
        clientRequests += (request.requestId -> sender())
        leader ! request
      }

    // appendEntry request from leader.
    // delegate to followerLogRep (not a actor) and return AppendResult to sender (LogSynchronizer actor)
    case entry : AppendEntries =>
      sender() ! followerLogRep.replicateLog(entry)

    // from LogSynchronizer . commit log, return response to client,  return CommitResult to leader
    case commit : CommitLog =>
      // return CommitResult to LogSynchronizer, then to LeaderLogRep, then to Leader.
      sender() ! commitLog(commit)

    case CleanAndClose =>
       cleanAndClose
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

    // no leader found. just tell client to re-try. this is the non-blocking fashion
    case request : ClientRequest =>
      sender ! Retry

    case CleanAndClose =>
      cleanAndClose
  }

  def leader(myTerm: Int, heartBeatSchr: Cancellable): Receive = {
    case Copy(term) =>
      checkStaleLeader(term, myTerm, heartBeatSchr)
    case RequestVote(term, candidateId) =>
      checkStaleLeader(term, myTerm, heartBeatSchr)
    case DoYouCopy(term) =>
      checkStaleLeader(term, myTerm, heartBeatSchr)

    // requests from client directly or indirectly (forwarded by followers)
    // note leader will never receive AppendEntries requests ( either from client or followers )
    case request : ClientRequest =>
      if(others.indexOf(sender()) < 0) // it comes from client directly, not from followers
        clientRequests.put(request.requestId, sender())

      if(leaderLogRepActor == null)
        leaderLogRepActor = context.actorOf(Props(new LeaderLogRep(others, logManager)))
      // wrap client request as LeaderRequest with leader's current term

      leaderLogRepActor ! LeaderRequest(request, myTerm)

    // commit log , send response to client, send result to self
    case e : CommitLog =>
      self ! commitLog(e)

    // commit results. do nothing.
    case result : CommitResult =>

    case CleanAndClose =>
      cleanAndClose
  }

  // TODO this should be an abstract method which depends on different implementations
  private def commitLog(e : CommitLog) : CommitResult = {
    // for instance, for ZooKeeper like application, this should change the status of znode
    // and return corresponding result according to different command types

    // for test purpose, here just print out the message to be committed
    log.info("log entry " + e.logIndex + " was committed : ) " + " by id = " + id)

    // TODO send response back to the clients
    // return result to client. in order to return response to client, it should be
    // able to find sender() from ClientRequest based on requestId. One possible solution
    // is including requestId in all messages, another way is to construct mapping between
    // e.logIndex and requestId.


    // return result to invoker
    CommitResult(e.logIndex, true)
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

  private def cleanAndClose = {
    // TODO for test only . print all logs before closing
    val identity = if(leader == self) "Leader" else "Follower"
    val sign = "id = " + this.id + ", identity = " + identity
    logManager.entrySet.foreach(e => {
      // log.info will disorder the output
      println(sign + "  key = " + e._1 + ", value = " + e._2.asInstanceOf[Entry])
    } )

    logManager.close
  }

  def receive = {
    case Tick =>
      becomeFollower(false, 0)
  }
}

object LeaderElection {
  val heartBeatPeriod = 200 milliseconds
  val electionTimeoutRange = 1000 to 2000

  var nodes : Seq[ActorRef] = _ // set externally
  val system = ActorSystem("raft")

  object Role extends Enumeration {
    type Role = Value
    val Follower, Candidate, Leader = Value
  }

  case class CleanAndClose()

  def start = nodes.foreach(_ ! Tick)

  // for test only
  def clean = {
    nodes.foreach(_ ! CleanAndClose)
  }

  def stop = system.shutdown()
}
  
  