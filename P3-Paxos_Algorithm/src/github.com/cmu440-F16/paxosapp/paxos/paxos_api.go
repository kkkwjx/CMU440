package paxos

import (
	"github.com/cmu440-F16/paxosapp/rpc/paxosrpc"
)

// GetNextProposalNumber generates a proposal number which will be passed to Propose.
// Propose initializes proposing a value for a key, and replies with the value that was committed for that key.
// GetValue looks up the value for a key, and replies with the value or with KeyNotFound.
type PaxosNode interface {
	GetNextProposalNumber(args *paxosrpc.ProposalNumberArgs, reply *paxosrpc.ProposalNumberReply) error
	Propose(args *paxosrpc.ProposeArgs, reply *paxosrpc.ProposeReply) error
	GetValue(args *paxosrpc.GetValueArgs, reply *paxosrpc.GetValueReply) error
}
