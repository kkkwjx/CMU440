package paxosrpc

// STAFF USE ONLY! Students should not use this interface in their code.
type RemotePaxosNode interface {
	// Called by servers using the paxos node.
	Propose(args *ProposeArgs, reply *ProposeReply) error
	GetValue(args *GetValueArgs, reply *GetValueReply) error
	GetNextProposalNumber(args *ProposalNumberArgs, reply *ProposalNumberReply) error

	// Called by other Paxos Nodes.
	RecvPrepare(args *PrepareArgs, reply *PrepareReply) error
	RecvAccept(args *AcceptArgs, reply *AcceptReply) error
	RecvCommit(args *CommitArgs, reply *CommitReply) error

	// Notify another node of a replacement server which
	// has started up.
	RecvReplaceServer(args *ReplaceServerArgs, reply *ReplaceServerReply) error

	// Request the value that was agreed upon for a particular round.
	RecvReplaceCatchup(args *ReplaceCatchupArgs, reply *ReplaceCatchupReply) error
}

type PaxosNode struct {
	// Embed all methods into the struct. See the Effective Go section about
	// embedding for more details: golang.org/doc/effective_go.html#embedding
	RemotePaxosNode
}

// Wrap wraps t in a type-safe wrapper struct to ensure that only the desired
// methods are exported to receive RPCs.
func Wrap(t RemotePaxosNode) RemotePaxosNode {
	return &PaxosNode{t}
}
