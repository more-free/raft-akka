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
	// TODO append log. set entry to a single String for easy test
	case class AppendEntries (term: Int, 
							 leaderId: Int, 
							 prevLogIndex : Int, 
							 prevLogTerm: Int, 
							 entry : AnyRef, 
							 leaderCommit: Int)

	case class AppendResult(term : Int, prevLogIndex : Int, success : Boolean)
	
	// two-phase commit
	// phase 1 , pre-commit (or "replicate logs") re-use the AppendEntries class
	// phase 2, commit
	case class CommitLog(prevLogIndex : Int)

  case class CommitResult(prevLogIndex : Int, success : Boolean)
}