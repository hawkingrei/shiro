package impo

func hasCandidate(v *MutateVisitor, name string) bool {
	if v == nil {
		return false
	}
	return len(v.Candidates[name]) > 0
}
