package raft

object Protocol {
	/** 
	 *  protocol for leader election 
	 */
	// heart beat
	case class DoYouCopy(term: Int)
	case class Copy(term: Int) 
							 
	// vote
	case class RequestVote(term: Int, candidateId: Int) // request voting for candidateId
	case class Vote(term: Int) // vote for candidateId

	/**
	 *   protocol for log replication 
	 */
	case class AppendEntries (
               term : Int,  // leader's term
							 prevLogIndex : Int, 
							 prevLogTerm: Int, 
							 entry : AnyRef
							 )

  /** prevLogIndex and prevLogTerm are set to the last entry in follower's DB */
  case class AppendResult(
               term : Int, // the same value as AppendEntries. that's necessary for async communication (rather than sync RPC)
               lastLogIndex : Int,
               lastLogTerm : Int,
               success : Boolean)

  // it is used for persistence only
	case class Entry (term : Int, entry : AnyRef)


	// two-phase commit
	// phase 1 , pre-commit (or "replicate logs") re-use the AppendEntries class
	// phase 2, commit
	case class CommitLog(logIndex : Int)

  // can also serve as response to client request (to replace FollowerResponse)
  case class CommitResult(lastLogIndex : Int, success : Boolean)


  /** protocol for client requests (it can be easily extended to complicated B/S protocols) */
  case class ClientRequest(requestId : Int, entry : AnyRef)
  case class ClientResponse(responseId : Int, result : AnyRef, status : Int) // state after appending entry
  case class Retry()  // leader has not been elected yet, ask client to retry instead of blocking the thread

  /** intermediate protocols */
  case class LeaderRequest(clientRequest : ClientRequest, term : Int)
  case class FollowerResponse(clientResponse : ClientResponse)
}