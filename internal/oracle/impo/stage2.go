package impo

import (
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	_ "github.com/pingcap/tidb/pkg/types/parser_driver"
	"github.com/pkg/errors"
)

// CalCandidates parses SQL and collects mutation candidates.
func CalCandidates(sql string) (*MutateVisitor, error) {
	p := parser.New()
	stmtNodes, _, err := p.Parse(sql, "", "")
	if err != nil {
		return nil, errors.Wrap(err, "impo candidates parse")
	}
	if len(stmtNodes) == 0 {
		return nil, errors.New("impo candidates empty statement")
	}
	rootNode := &stmtNodes[0]
	v := &MutateVisitor{
		Root:       *rootNode,
		Candidates: make(map[string][]*Candidate),
	}
	v.visit(*rootNode, 1)
	return v, nil
}

// Mutate applies one candidate mutation and returns the mutated SQL.
func Mutate(rootNode ast.Node, candidate *Candidate, seed int64) (string, error) {
	var (
		sql []byte
		err error
	)
	switch candidate.MutationName {
	case FixMDistinctU:
		sql, err = doFixMDistinctU(rootNode, candidate.Node)
	case FixMDistinctL:
		sql, err = doFixMDistinctL(rootNode, candidate.Node)
	case FixMCmpOpU:
		sql, err = doFixMCmpOpU(rootNode, candidate.Node)
	case FixMCmpOpL:
		sql, err = doFixMCmpOpL(rootNode, candidate.Node)
	case FixMAnyAllU:
		sql, err = doFixMAnyAllU(rootNode, candidate.Node)
	case FixMAnyAllL:
		sql, err = doFixMAnyAllL(rootNode, candidate.Node)
	case FixMUnionAllU:
		sql, err = doFixMUnionAllU(rootNode, candidate.Node)
	case FixMUnionAllL:
		sql, err = doFixMUnionAllL(rootNode, candidate.Node)
	case FixMInNullU:
		sql, err = doFixMInNullU(rootNode, candidate.Node)
	case FixMInListU:
		sql, err = doFixMInListU(rootNode, candidate.Node)
	case FixMInListL:
		sql, err = doFixMInListL(rootNode, candidate.Node)
	case FixMBetweenU:
		sql, err = doFixMBetweenU(rootNode, candidate.Node)
	case FixMBetweenL:
		sql, err = doFixMBetweenL(rootNode, candidate.Node)
	case FixMWhere1U:
		sql, err = doFixMWhere1U(rootNode, candidate.Node)
	case FixMWhere0L:
		sql, err = doFixMWhere0L(rootNode, candidate.Node)
	case FixMExistsU:
		sql, err = doFixMExistsU(rootNode, candidate.Node)
	case FixMExistsL:
		sql, err = doFixMExistsL(rootNode, candidate.Node)
	case FixMHaving1U:
		sql, err = doFixMHaving1U(rootNode, candidate.Node)
	case FixMHaving0L:
		sql, err = doFixMHaving0L(rootNode, candidate.Node)
	case FixMOn1U:
		sql, err = doFixMOn1U(rootNode, candidate.Node)
	case FixMOn0L:
		sql, err = doFixMOn0L(rootNode, candidate.Node)
	case FixMRmUnionAllL:
		sql, err = doFixMRmUnionAllL(rootNode, candidate.Node)
	case FixMRmUnionL:
		sql, err = doFixMRmUnionL(rootNode, candidate.Node)
	case FixMRmOrderByL:
		sql, err = doFixMRmOrderByL(rootNode, candidate.Node)
	case FixMLimitU:
		sql, err = doFixMLimitU(rootNode, candidate.Node)
	case RdMLikeU:
		sql, err = doRdMLikeU(rootNode, candidate.Node, seed)
	case RdMLikeL:
		sql, err = doRdMLikeL(rootNode, candidate.Node, seed)
	case RdMRegExpU:
		sql, err = doRdMRegExpU(rootNode, candidate.Node, seed)
	case RdMRegExpL:
		sql, err = doRdMRegExpL(rootNode, candidate.Node, seed)
	default:
		return "", errors.New("impo unknown mutation")
	}
	if err != nil {
		return "", err
	}
	return string(sql), nil
}

// MutateUnit holds one mutation result.
type MutateUnit struct {
	Name    string
	SQL     string
	IsUpper bool
	Err     error
}

// MutateResult holds all mutation units.
type MutateResult struct {
	MutateUnits []*MutateUnit
	Err         error
}

// MutateAll applies all candidates and collects mutated SQLs.
func MutateAll(sql string, seed int64) *MutateResult {
	mutateResult := &MutateResult{MutateUnits: make([]*MutateUnit, 0)}
	v, err := CalCandidates(sql)
	if err != nil {
		mutateResult.Err = err
		return mutateResult
	}
	root := v.Root
	for mutationName, candidateList := range v.Candidates {
		for _, candidate := range candidateList {
			newSQL, err := Mutate(root, candidate, seed)
			mutateResult.MutateUnits = append(mutateResult.MutateUnits, &MutateUnit{
				Name:    mutationName,
				SQL:     newSQL,
				IsUpper: ((candidate.U ^ candidate.Flag) ^ 1) == 1,
				Err:     err,
			})
		}
	}
	return mutateResult
}
