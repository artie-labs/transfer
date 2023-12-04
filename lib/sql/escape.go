package sql

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/artie-labs/transfer/lib/config"

	"github.com/artie-labs/transfer/lib/array"
	"github.com/artie-labs/transfer/lib/config/constants"
)

type NameArgs struct {
	Escape   bool
	DestKind constants.DestinationKind
}

// symbolsToEscape are additional keywords that we need to escape
var symbolsToEscape = []string{":"}

func EscapeName(ctx context.Context, name string, args *NameArgs) string {
	if args == nil {
		return name
	}

	var reservedKeywords []string
	if args.DestKind == constants.Redshift {
		reservedKeywords = constants.RedshiftReservedKeywords
	} else {
		reservedKeywords = constants.ReservedKeywords
	}

	needsEscaping := array.StringContains(reservedKeywords, name)

	// If it does not contain any reserved words, does it contain any symbols that need to be escaped?
	if !needsEscaping {
		for _, symbol := range symbolsToEscape {
			if strings.Contains(name, symbol) {
				needsEscaping = true
				break
			}
		}
	}

	// If it still doesn't need to be escaped, we should check if it's a number.
	if !needsEscaping {
		if _, err := strconv.Atoi(name); err == nil {
			needsEscaping = true
		}
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
