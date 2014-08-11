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
							 entry : Serializable, 
							 leaderCommit: Int)
	case class AppendResult(term : Int, success : Boolean)
	
	// two-phase commit
	case class Prepare
	case class Commit
}