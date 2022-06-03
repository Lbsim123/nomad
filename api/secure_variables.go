package api

import (
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"
)

const (
	ErrVariableNotFound     = "secure variable not found"
	ErrVariableMissingItems = "secure variable missing Items field"
)

// SecureVariables is used to access secure variables.
type SecureVariables struct {
	client *Client
}

// SecureVariables returns a new handle on the secure variables.
func (c *Client) SecureVariables() *SecureVariables {
	return &SecureVariables{client: c}
}

// Create is used to create a secure variable.
func (sv *SecureVariables) Create(v *SecureVariable, qo *WriteOptions) (*WriteMeta, error) {

	v.Path = cleanPathString(v.Path)
	wm, err := sv.client.write("/v1/var/"+v.Path, v, nil, qo)
	if err != nil {
		return nil, err
	}
	return wm, nil
}

// Read is used to query a single secure variable by path.
func (sv *SecureVariables) Read(path string, qo *QueryOptions) (*SecureVariable, *QueryMeta, error) {

	path = cleanPathString(path)
	var svar = new(SecureVariable)
	qm, err := sv.readInternal("/v1/var/"+path, &svar, qo)
	if err != nil {
		return nil, nil, err
	}
	if svar == nil {
		return nil, qm, errors.New(ErrVariableNotFound)
	}
	return svar, qm, nil
}

// Peek is used to query a single secure variable by path, but will not error on
// not found.
func (sv *SecureVariables) Peek(path string, qo *QueryOptions) (*SecureVariable, *QueryMeta, error) {

	path = cleanPathString(path)
	var svar = new(SecureVariable)
	qm, err := sv.readInternal("/v1/var/"+path, &svar, qo)
	if err != nil {
		return nil, nil, err
	}
	return svar, qm, nil
}

// Update is used to update a secure variable.
func (sv *SecureVariables) Update(v *SecureVariable, qo *WriteOptions) (*WriteMeta, error) {

	v.Path = cleanPathString(v.Path)
	wm, err := sv.client.write("/v1/var/"+v.Path, v, nil, qo)
	if err != nil {
		return nil, err
	}
	return wm, nil
}

// CheckedUpdate is used to updated a secure variable if the modify index
// matches the one on the server
func (sv *SecureVariables) CheckedUpdate(v *SecureVariable, qo *WriteOptions) (*WriteMeta, error) {

	result := new(SecureVariable)
	v.Path = cleanPathString(v.Path)
	wm, err := sv.client.write("/v1/var/"+v.Path+"?cas="+fmt.Sprint(v.ModifyIndex), v, result, qo)
	if err != nil {
		return nil, err
	}

	if isConflict(v.ModifyIndex, result) {
		return nil, ErrCASConflict{
			cIdx:     v.ModifyIndex,
			conflict: result,
		}
	}
	return wm, nil
}

// Delete is used to delete a secure variable
func (sv *SecureVariables) Delete(path string, qo *WriteOptions) (*WriteMeta, error) {

	path = cleanPathString(path)
	wm, err := sv.client.delete(fmt.Sprintf("/v1/var/%s", path), nil, qo)
	if err != nil {
		return nil, err
	}
	return wm, nil
}

// CheckedDelete is used to conditionally delete a secure variable
func (sv *SecureVariables) CheckedDelete(path string, checkIndex uint64, qo *WriteOptions) (*WriteMeta, error) {

	result := new(SecureVariable)
	path = cleanPathString(path)
	wm, err := sv.client.delete(fmt.Sprintf("/v1/var/%s?cas=%v", path, checkIndex), &result, qo)
	if err != nil {
		return nil, err
	}

	if isConflict(checkIndex, result) {
		return nil, ErrCASConflict{
			cIdx:     checkIndex,
			conflict: result,
		}
	}
	return wm, nil
}

func isConflict(checkIndex uint64, conflict *SecureVariable) bool {

	if conflict == nil {
		return false
	}
	return checkIndex != conflict.ModifyIndex
}

// List is used to dump all of the secure variables, can be used to pass prefix
// via QueryOptions rather than as a parameter
func (sv *SecureVariables) List(qo *QueryOptions) ([]*SecureVariableMetadata, *QueryMeta, error) {

	var resp []*SecureVariableMetadata
	qm, err := sv.client.query("/v1/vars", &resp, qo)
	if err != nil {
		return nil, nil, err
	}
	return resp, qm, nil
}

// PrefixList is used to do a PrefixList search over secure variables
func (sv *SecureVariables) PrefixList(prefix string, qo *QueryOptions) ([]*SecureVariableMetadata, *QueryMeta, error) {

	if qo == nil {
		qo = &QueryOptions{Prefix: prefix}
	} else {
		qo.Prefix = prefix
	}

	return sv.List(qo)
}

// GetItems returns the inner Items collection from a secure variable at a
// given path
func (sv *SecureVariables) GetItems(path string, qo *QueryOptions) (*SecureVariableItems, *QueryMeta, error) {

	path = cleanPathString(path)
	svar := new(SecureVariable)

	qm, err := sv.readInternal("/v1/var/"+path, &svar, qo)
	if err != nil {
		return nil, nil, err
	}

	return &svar.Items, qm, nil
}

func (sv *SecureVariables) readInternal(endpoint string, out **SecureVariable, q *QueryOptions) (*QueryMeta, error) {

	r, err := sv.client.newRequest("GET", endpoint)
	if err != nil {
		return nil, err
	}
	r.setQueryOptions(q)
	rtt, resp, err := requireOKOrNotFound(sv.client.doRequest(r))
	if err != nil {
		return nil, err
	}

	qm := &QueryMeta{}
	parseQueryMeta(resp, qm)
	qm.RequestTime = rtt

	if resp.StatusCode == http.StatusNotFound {
		*out = nil
		resp.Body.Close()
		return qm, nil
	}

	defer resp.Body.Close()
	if err := decodeBody(resp, out); err != nil {
		fmt.Println(err.Error())
		return nil, err
	}

	return qm, nil
}

func requireOKOrNotFound(d time.Duration, resp *http.Response, e error) (time.Duration, *http.Response, error) {

	fn := func(s int) bool {
		return s == http.StatusOK || s == http.StatusNotFound
	}
	return requireStatusFn(d, resp, e, fn)
}

// SecureVariable specifies the metadata and contents to be stored in the
// encrypted Nomad backend.
type SecureVariable struct {
	// Namespace is the Nomad namespace associated with the secure variable
	Namespace string
	// Path is the path to the secure variable
	Path string

	// Raft indexes to track creation and modification
	CreateIndex uint64
	ModifyIndex uint64

	// Times provided as a convenience for operators expressed time.UnixNanos
	CreateTime int64
	ModifyTime int64

	Items SecureVariableItems
}

// SecureVariableMetadata specifies the metadata for a secure variable and
// is used as the list object
type SecureVariableMetadata struct {
	// Namespace is the Nomad namespace associated with the secure variable
	Namespace string
	// Path is the path to the secure variable
	Path string

	// Raft indexes to track creation and modification
	CreateIndex uint64
	ModifyIndex uint64

	// Times provided as a convenience for operators expressed time.UnixNanos
	CreateTime int64
	ModifyTime int64
}

type SecureVariableItems map[string]string

// NewSecureVariable is a convenience method to more easily create a
// ready-to-use secure variable
func NewSecureVariable(path string) *SecureVariable {

	return &SecureVariable{
		Path:  path,
		Items: make(SecureVariableItems),
	}
}

func (sv1 *SecureVariable) Copy() *SecureVariable {

	var out SecureVariable = *sv1
	out.Items = make(SecureVariableItems)
	for k, v := range sv1.Items {
		out.Items[k] = v
	}
	return &out
}

func (sv *SecureVariable) Metadata() *SecureVariableMetadata {

	return &SecureVariableMetadata{
		Namespace:   sv.Namespace,
		Path:        sv.Path,
		CreateIndex: sv.CreateIndex,
		ModifyIndex: sv.ModifyIndex,
		CreateTime:  sv.CreateTime,
		ModifyTime:  sv.ModifyTime,
	}
}

func (sv *SecureVariable) IsZeroValue() bool {
	return *sv.Metadata() == SecureVariableMetadata{} && sv.Items == nil
}

func cleanPathString(path string) string {
	return strings.Trim(path, " /")
}

type ErrCASConflict struct {
	cIdx     uint64
	conflict *SecureVariable
}

func (e ErrCASConflict) Error() string {
	return fmt.Sprintf("cas conflict: expected ModifyIndex %v; found %v", e.cIdx, e.conflict.ModifyIndex)
}
