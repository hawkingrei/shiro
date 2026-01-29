package oracle

import "strings"

func builderSkipReason(prefix string, reason string) string {
	if reason == "" {
		return prefix + ":builder"
	}
	sanitized := strings.NewReplacer(":", "_", " ", "_").Replace(reason)
	return prefix + ":builder_" + sanitized
}
