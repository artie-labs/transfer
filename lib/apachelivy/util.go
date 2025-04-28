package apachelivy

import (
	"net/http"
	"slices"
	"strings"
)

const sessionBufferSeconds = 30

func shouldCreateNewSession(resp GetSessionResponse, statusCode int, err error) (bool, error) {
	if statusCode == http.StatusNotFound {
		return true, nil
	}

	if err != nil {
		return false, err
	}

	// If the session is in a terminal state, then we should create a new one.
	return slices.Contains(TerminalSessionStates, resp.State), nil
}

func shouldRetryError(err error) bool {
	if err == nil {
		return false
	}

	if strings.Contains(err.Error(), "TABLE_OR_VIEW_NOT_FOUND") {
		return false
	}

	return true
}
