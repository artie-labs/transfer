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
	if args == nil {
		return name
	}

	var needsEscaping bool
	if args.DestKind == constants.Redshift {
		needsEscaping = array.StringContains(constants.RedshiftReservedKeywords, name)
	} else {
		needsEscaping = array.StringContains(constants.ReservedKeywords, name)
	}

	if args.Escape && needsEscaping {
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
