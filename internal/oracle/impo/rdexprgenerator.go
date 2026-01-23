package impo

import (
	"github.com/pingcap/tidb/pkg/parser/test_driver"

	"math/rand"
)

var charset []byte = []byte("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz1234567890!@#$%^&*()-=_+~[]{};:,./<>?|")

// GenRandomStr generates a random string with length n.
func GenRandomStr(n int, seed int64) string {
	rander := rand.New(rand.NewSource(seed))
	result := make([]byte, n)
	for i := 0; i < n; i++ {
		result[i] = charset[rander.Intn(len(charset))]
	}
	return string(result)
}

// GenRandomValueExpr generates n random value exprs: nil, int63, float64, string(len 1~10).
func GenRandomValueExpr(n int, seed int64) []*test_driver.ValueExpr {
	rander := rand.New(rand.NewSource(seed))
	res := make([]*test_driver.ValueExpr, 0)
	for i := 0; i < n; i++ {
		rd := rander.Intn(4)
		var data interface{}
		switch rd {
		case 0:
			data = nil
		case 1:
			data = rander.Int63()
		case 2:
			data = rander.Float64()
		case 3:
			data = GenRandomStr(rander.Intn(10)+1, seed+int64(i))
		}
		res = append(res, &test_driver.ValueExpr{
			Datum: test_driver.NewDatum(data),
		})
	}
	return res
}
