package impo

// Mutation names supported by Pinolo-style approximations.
const (
	FixMDistinctU = "FixMDistinctU"
	FixMDistinctL = "FixMDistinctL"

	FixMUnionAllU = "FixMUnionAllU"
	FixMUnionAllL = "FixMUnionAllL"

	FixMCmpOpU = "FixMCmpOpU"
	FixMCmpOpL = "FixMCmpOpL"

	FixMInNullU = "FixMInNullU"

	FixMWhere1U = "FixMWhere1U"
	FixMWhere0L = "FixMWhere0L"

	FixMHaving1U = "FixMHaving1U"
	FixMHaving0L = "FixMHaving0L"

	FixMOn1U = "FixMOn1U"
	FixMOn0L = "FixMOn0L"

	FixMRmUnionAllL = "FixMRmUnionAllL"

	RdMLikeU = "RdMLikeU"
	RdMLikeL = "RdMLikeL"

	RdMRegExpU = "RdMRegExpU"
	RdMRegExpL = "RdMRegExpL"
)
