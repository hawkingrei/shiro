package oracle

import (
	"sync"
	"testing"
)

func TestSubqueryFeatureLRUEviction(t *testing.T) {
	cache := newSubqueryFeatureLRU(2)
	cache.add("a", SQLSubqueryFeatures{HasInList: true})
	cache.add("b", SQLSubqueryFeatures{HasNotInList: true})
	if _, ok := cache.get("a"); !ok {
		t.Fatalf("expected cache hit for a")
	}
	cache.add("c", SQLSubqueryFeatures{HasExistsSubquery: true})
	if _, ok := cache.get("b"); ok {
		t.Fatalf("expected cache miss for evicted key b")
	}
	if _, ok := cache.get("a"); !ok {
		t.Fatalf("expected cache hit for a after eviction")
	}
	if _, ok := cache.get("c"); !ok {
		t.Fatalf("expected cache hit for c")
	}
}

func TestSubqueryFeatureCacheParseError(t *testing.T) {
	orig := subqueryFeatureCache
	subqueryFeatureCache = newSubqueryFeatureLRU(1)
	defer func() {
		subqueryFeatureCache = orig
	}()

	sql := "SELECT FROM"
	_ = DetectSubqueryFeaturesSQL(sql)
	if _, ok := subqueryFeatureCache.get(sql); !ok {
		t.Fatalf("expected parse error result to be cached")
	}
}

func TestSubqueryFeatureLRUConcurrentAccess(_ *testing.T) {
	cache := newSubqueryFeatureLRU(8)
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			key := string(rune('a' + idx%8))
			cache.add(key, SQLSubqueryFeatures{HasInList: true})
			_, _ = cache.get(key)
		}(i)
	}
	wg.Wait()
}
