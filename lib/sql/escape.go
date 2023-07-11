package sql

import (
	"context"
	"fmt"
	"strings"

	"github.com/artie-labs/transfer/lib/config"

	"github.com/artie-labs/transfer/lib/array"
	"github.com/artie-labs/transfer/lib/config/constants"
)

type NameArgs struct {
	Escape   bool
	DestKind constants.DestinationKind
}

func EscapeName(ctx context.Context, name string, args *NameArgs) string {
	var escape bool
	if args != nil {
		escape = args.Escape
	}

	if escape && array.StringContains(constants.ReservedKeywords, name) {
		if config.FromContext(ctx).Config.SharedDestinationConfig.UppercaseEscapedNames {
			name = strings.ToUpper(name)
		}

		if args != nil && args.DestKind == constants.BigQuery {
			// BigQuery needs backticks to escape.
			return fmt.Sprintf("`%s`", name)
		} else {
			// Snowflake uses quotes.
			return fmt.Sprintf(`"%s"`, name)
		}
	}

	return name
}
