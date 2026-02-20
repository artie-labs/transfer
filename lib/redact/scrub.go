package redact

import (
	"regexp"
)

type scrubRule struct {
	pattern     *regexp.Regexp
	replacement string
	replaceFn   func(string) string
}

// URI with embedded credentials: scheme://user:password@host
var uriPasswordPattern = regexp.MustCompile(`([a-zA-Z][a-zA-Z0-9+\-.]*://[^:@/\s]*):([^@\s]+)@`)

// Key-value patterns for secrets: "key": "value" (JSON-style)
var quotedKeyValuePattern = regexp.MustCompile(`(?i)("(?:password|passwd|secret|token|api_key|apikey|access_key|auth|credential|private_key)")\s*:\s*"([^"]+)"`)

// Key-value patterns: key=value or key: value (unquoted)
var unquotedKeyValuePattern = regexp.MustCompile(`(?i)((?:password|passwd|secret|token|api_key|apikey|access_key|auth|credential|private_key)\s*[:=]\s*)((?:Bearer\s+)?[^\s,;&}"]+)`)

var scrubRules = []scrubRule{
	{
		pattern:   uriPasswordPattern,
		replaceFn: scrubURIPassword,
	},
	{
		pattern:     quotedKeyValuePattern,
		replacement: `${1}: "[REDACTED]"`,
	},
	{
		pattern:     unquotedKeyValuePattern,
		replacement: `${1}[REDACTED]`,
	},
	{
		pattern:     regexp.MustCompile(`\bAKIA[0-9A-Z]{16}\b`),
		replacement: "[REDACTED]",
	},
	{
		pattern:     regexp.MustCompile(`(?i)\bBearer\s+[A-Za-z0-9\-._~+/]+=*`),
		replacement: "Bearer [REDACTED]",
	},
	{
		pattern:     regexp.MustCompile(`(?s)-{5}BEGIN[A-Z\s]*PRIVATE\s+KEY-{5}.+?-{5}END[A-Z\s]*PRIVATE\s+KEY-{5}`),
		replacement: "[REDACTED]",
	},
	{
		pattern:     regexp.MustCompile(`[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}`),
		replacement: "[REDACTED]",
	},
	{
		pattern:     regexp.MustCompile(`\b\d{3}[- ]\d{2}[- ]\d{4}\b`),
		replacement: "[REDACTED]",
	},
	{
		pattern:     regexp.MustCompile(`\b\d{4}[\s-]?\d{4}[\s-]?\d{4}[\s-]?\d{4}\b`),
		replacement: "[REDACTED]",
	},
}

// ScrubErrorMessage redacts credentials, secrets, and PII/PHI patterns from an error message string.
func ScrubErrorMessage(msg string) string {
	for _, rule := range scrubRules {
		if rule.replaceFn != nil {
			msg = rule.pattern.ReplaceAllStringFunc(msg, rule.replaceFn)
		} else {
			msg = rule.pattern.ReplaceAllString(msg, rule.replacement)
		}
	}
	return msg
}

func scrubURIPassword(match string) string {
	return uriPasswordPattern.ReplaceAllString(match, `${1}:[REDACTED]@`)
}
