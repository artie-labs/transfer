// Code generated by counterfeiter. DO NOT EDIT.
package mocks

import (
	"database/sql"
	"sync"

	"github.com/artie-labs/transfer/lib/db"
)

type FakeStore struct {
	BeginStub        func() (*sql.Tx, error)
	beginMutex       sync.RWMutex
	beginArgsForCall []struct {
	}
	beginReturns struct {
		result1 *sql.Tx
		result2 error
	}
	beginReturnsOnCall map[int]struct {
		result1 *sql.Tx
		result2 error
	}
	ExecStub        func(string, ...any) (sql.Result, error)
	execMutex       sync.RWMutex
	execArgsForCall []struct {
		arg1 string
		arg2 []any
	}
	execReturns struct {
		result1 sql.Result
		result2 error
	}
	execReturnsOnCall map[int]struct {
		result1 sql.Result
		result2 error
	}
	IsRetryableErrorStub        func(error) bool
	isRetryableErrorMutex       sync.RWMutex
	isRetryableErrorArgsForCall []struct {
		arg1 error
	}
	isRetryableErrorReturns struct {
		result1 bool
	}
	isRetryableErrorReturnsOnCall map[int]struct {
		result1 bool
	}
	QueryStub        func(string, ...any) (*sql.Rows, error)
	queryMutex       sync.RWMutex
	queryArgsForCall []struct {
		arg1 string
		arg2 []any
	}
	queryReturns struct {
		result1 *sql.Rows
		result2 error
	}
	queryReturnsOnCall map[int]struct {
		result1 *sql.Rows
		result2 error
	}
	invocations      map[string][][]interface{}
	invocationsMutex sync.RWMutex
}

func (fake *FakeStore) Begin() (*sql.Tx, error) {
	fake.beginMutex.Lock()
	ret, specificReturn := fake.beginReturnsOnCall[len(fake.beginArgsForCall)]
	fake.beginArgsForCall = append(fake.beginArgsForCall, struct {
	}{})
	stub := fake.BeginStub
	fakeReturns := fake.beginReturns
	fake.recordInvocation("Begin", []interface{}{})
	fake.beginMutex.Unlock()
	if stub != nil {
		return stub()
	}
	if specificReturn {
		return ret.result1, ret.result2
	}
	return fakeReturns.result1, fakeReturns.result2
}

func (fake *FakeStore) BeginCallCount() int {
	fake.beginMutex.RLock()
	defer fake.beginMutex.RUnlock()
	return len(fake.beginArgsForCall)
}

func (fake *FakeStore) BeginCalls(stub func() (*sql.Tx, error)) {
	fake.beginMutex.Lock()
	defer fake.beginMutex.Unlock()
	fake.BeginStub = stub
}

func (fake *FakeStore) BeginReturns(result1 *sql.Tx, result2 error) {
	fake.beginMutex.Lock()
	defer fake.beginMutex.Unlock()
	fake.BeginStub = nil
	fake.beginReturns = struct {
		result1 *sql.Tx
		result2 error
	}{result1, result2}
}

func (fake *FakeStore) BeginReturnsOnCall(i int, result1 *sql.Tx, result2 error) {
	fake.beginMutex.Lock()
	defer fake.beginMutex.Unlock()
	fake.BeginStub = nil
	if fake.beginReturnsOnCall == nil {
		fake.beginReturnsOnCall = make(map[int]struct {
			result1 *sql.Tx
			result2 error
		})
	}
	fake.beginReturnsOnCall[i] = struct {
		result1 *sql.Tx
		result2 error
	}{result1, result2}
}

func (fake *FakeStore) Exec(arg1 string, arg2 ...any) (sql.Result, error) {
	fake.execMutex.Lock()
	ret, specificReturn := fake.execReturnsOnCall[len(fake.execArgsForCall)]
	fake.execArgsForCall = append(fake.execArgsForCall, struct {
		arg1 string
		arg2 []any
	}{arg1, arg2})
	stub := fake.ExecStub
	fakeReturns := fake.execReturns
	fake.recordInvocation("Exec", []interface{}{arg1, arg2})
	fake.execMutex.Unlock()
	if stub != nil {
		return stub(arg1, arg2...)
	}
	if specificReturn {
		return ret.result1, ret.result2
	}
	return fakeReturns.result1, fakeReturns.result2
}

func (fake *FakeStore) ExecCallCount() int {
	fake.execMutex.RLock()
	defer fake.execMutex.RUnlock()
	return len(fake.execArgsForCall)
}

func (fake *FakeStore) ExecCalls(stub func(string, ...any) (sql.Result, error)) {
	fake.execMutex.Lock()
	defer fake.execMutex.Unlock()
	fake.ExecStub = stub
}

func (fake *FakeStore) ExecArgsForCall(i int) (string, []any) {
	fake.execMutex.RLock()
	defer fake.execMutex.RUnlock()
	argsForCall := fake.execArgsForCall[i]
	return argsForCall.arg1, argsForCall.arg2
}

func (fake *FakeStore) ExecReturns(result1 sql.Result, result2 error) {
	fake.execMutex.Lock()
	defer fake.execMutex.Unlock()
	fake.ExecStub = nil
	fake.execReturns = struct {
		result1 sql.Result
		result2 error
	}{result1, result2}
}

func (fake *FakeStore) ExecReturnsOnCall(i int, result1 sql.Result, result2 error) {
	fake.execMutex.Lock()
	defer fake.execMutex.Unlock()
	fake.ExecStub = nil
	if fake.execReturnsOnCall == nil {
		fake.execReturnsOnCall = make(map[int]struct {
			result1 sql.Result
			result2 error
		})
	}
	fake.execReturnsOnCall[i] = struct {
		result1 sql.Result
		result2 error
	}{result1, result2}
}

func (fake *FakeStore) IsRetryableError(arg1 error) bool {
	fake.isRetryableErrorMutex.Lock()
	ret, specificReturn := fake.isRetryableErrorReturnsOnCall[len(fake.isRetryableErrorArgsForCall)]
	fake.isRetryableErrorArgsForCall = append(fake.isRetryableErrorArgsForCall, struct {
		arg1 error
	}{arg1})
	stub := fake.IsRetryableErrorStub
	fakeReturns := fake.isRetryableErrorReturns
	fake.recordInvocation("IsRetryableError", []interface{}{arg1})
	fake.isRetryableErrorMutex.Unlock()
	if stub != nil {
		return stub(arg1)
	}
	if specificReturn {
		return ret.result1
	}
	return fakeReturns.result1
}

func (fake *FakeStore) IsRetryableErrorCallCount() int {
	fake.isRetryableErrorMutex.RLock()
	defer fake.isRetryableErrorMutex.RUnlock()
	return len(fake.isRetryableErrorArgsForCall)
}

func (fake *FakeStore) IsRetryableErrorCalls(stub func(error) bool) {
	fake.isRetryableErrorMutex.Lock()
	defer fake.isRetryableErrorMutex.Unlock()
	fake.IsRetryableErrorStub = stub
}

func (fake *FakeStore) IsRetryableErrorArgsForCall(i int) error {
	fake.isRetryableErrorMutex.RLock()
	defer fake.isRetryableErrorMutex.RUnlock()
	argsForCall := fake.isRetryableErrorArgsForCall[i]
	return argsForCall.arg1
}

func (fake *FakeStore) IsRetryableErrorReturns(result1 bool) {
	fake.isRetryableErrorMutex.Lock()
	defer fake.isRetryableErrorMutex.Unlock()
	fake.IsRetryableErrorStub = nil
	fake.isRetryableErrorReturns = struct {
		result1 bool
	}{result1}
}

func (fake *FakeStore) IsRetryableErrorReturnsOnCall(i int, result1 bool) {
	fake.isRetryableErrorMutex.Lock()
	defer fake.isRetryableErrorMutex.Unlock()
	fake.IsRetryableErrorStub = nil
	if fake.isRetryableErrorReturnsOnCall == nil {
		fake.isRetryableErrorReturnsOnCall = make(map[int]struct {
			result1 bool
		})
	}
	fake.isRetryableErrorReturnsOnCall[i] = struct {
		result1 bool
	}{result1}
}

func (fake *FakeStore) Query(arg1 string, arg2 ...any) (*sql.Rows, error) {
	fake.queryMutex.Lock()
	ret, specificReturn := fake.queryReturnsOnCall[len(fake.queryArgsForCall)]
	fake.queryArgsForCall = append(fake.queryArgsForCall, struct {
		arg1 string
		arg2 []any
	}{arg1, arg2})
	stub := fake.QueryStub
	fakeReturns := fake.queryReturns
	fake.recordInvocation("Query", []interface{}{arg1, arg2})
	fake.queryMutex.Unlock()
	if stub != nil {
		return stub(arg1, arg2...)
	}
	if specificReturn {
		return ret.result1, ret.result2
	}
	return fakeReturns.result1, fakeReturns.result2
}

func (fake *FakeStore) QueryCallCount() int {
	fake.queryMutex.RLock()
	defer fake.queryMutex.RUnlock()
	return len(fake.queryArgsForCall)
}

func (fake *FakeStore) QueryCalls(stub func(string, ...any) (*sql.Rows, error)) {
	fake.queryMutex.Lock()
	defer fake.queryMutex.Unlock()
	fake.QueryStub = stub
}

func (fake *FakeStore) QueryArgsForCall(i int) (string, []any) {
	fake.queryMutex.RLock()
	defer fake.queryMutex.RUnlock()
	argsForCall := fake.queryArgsForCall[i]
	return argsForCall.arg1, argsForCall.arg2
}

func (fake *FakeStore) QueryReturns(result1 *sql.Rows, result2 error) {
	fake.queryMutex.Lock()
	defer fake.queryMutex.Unlock()
	fake.QueryStub = nil
	fake.queryReturns = struct {
		result1 *sql.Rows
		result2 error
	}{result1, result2}
}

func (fake *FakeStore) QueryReturnsOnCall(i int, result1 *sql.Rows, result2 error) {
	fake.queryMutex.Lock()
	defer fake.queryMutex.Unlock()
	fake.QueryStub = nil
	if fake.queryReturnsOnCall == nil {
		fake.queryReturnsOnCall = make(map[int]struct {
			result1 *sql.Rows
			result2 error
		})
	}
	fake.queryReturnsOnCall[i] = struct {
		result1 *sql.Rows
		result2 error
	}{result1, result2}
}

func (fake *FakeStore) Invocations() map[string][][]interface{} {
	fake.invocationsMutex.RLock()
	defer fake.invocationsMutex.RUnlock()
	fake.beginMutex.RLock()
	defer fake.beginMutex.RUnlock()
	fake.execMutex.RLock()
	defer fake.execMutex.RUnlock()
	fake.isRetryableErrorMutex.RLock()
	defer fake.isRetryableErrorMutex.RUnlock()
	fake.queryMutex.RLock()
	defer fake.queryMutex.RUnlock()
	copiedInvocations := map[string][][]interface{}{}
	for key, value := range fake.invocations {
		copiedInvocations[key] = value
	}
	return copiedInvocations
}

func (fake *FakeStore) recordInvocation(key string, args []interface{}) {
	fake.invocationsMutex.Lock()
	defer fake.invocationsMutex.Unlock()
	if fake.invocations == nil {
		fake.invocations = map[string][][]interface{}{}
	}
	if fake.invocations[key] == nil {
		fake.invocations[key] = [][]interface{}{}
	}
	fake.invocations[key] = append(fake.invocations[key], args)
}

var _ db.Store = new(FakeStore)
