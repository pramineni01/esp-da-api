package datamodels

import (
	"database/sql"
	"fmt"
	"io"
	"strconv"
)

type DAQueryInput struct {
	DatasetID          string                       `json:"datasetID"`
	Datatable          string                       `json:"datatable"`
	BranchID           *string                      `json:"branchID"`
	Version            *string                      `json:"version"`
	Scope              *DAQueryScopeInput           `json:"scope"`
	DimensionLevels    []string                     `json:"dimensionLevels"`
	AggregatedMeasures []string                     `json:"aggregatedMeasures"`
	PostAggFilter      *DAQueryPostAggFilterInput   `json:"postAggFilter"`
	Sort               *DAQuerySortInput            `json:"sort"`
	PostAggGrouping    *DAQueryPostAggGroupingInput `json:"postAggGrouping"`
	First              *int                         `json:"first"`
	After              *string                      `json:"after"`
}

type DADimMembersQueryInput struct {
	DatasetID       string              `json:"datasetID"`
	Datatable       string              `json:"datatable"`
	BranchID        *string             `json:"branchID"`
	Version         *string             `json:"version"`
	Scope           *DAQueryScopeInput  `json:"scope"`
	Search          *DAQuerySearchInput `json:"search"`
	DimensionLevels []string            `json:"dimensionLevels"`
	Sort            *DAQuerySortInput   `json:"sort"`
	First           *int                `json:"first"`
	After           *string             `json:"after"`
}

type DAPageInfo struct {
	HasNextPage bool `json:"hasNextPage"`

	// internal usage
	queryId    string
	countQuery string
	datasetID  int
}

type DAComparisonOperator string

const (
	DAComparisonOperatorIn DAComparisonOperator = "IN"
)

var AllDAComparisonOperator = []DAComparisonOperator{
	DAComparisonOperatorIn,
}

func (e DAComparisonOperator) IsValid() bool {
	switch e {
	case DAComparisonOperatorIn:
		return true
	}
	return false
}

func (e DAComparisonOperator) String() string {
	return string(e)
}

func (e *DAComparisonOperator) UnmarshalGQL(v interface{}) error {
	str, ok := v.(string)
	if !ok {
		return fmt.Errorf("enums must be strings")
	}

	*e = DAComparisonOperator(str)
	if !e.IsValid() {
		return fmt.Errorf("%s is not a valid DAComparisonOperator", str)
	}
	return nil
}

func (e DAComparisonOperator) MarshalGQL(w io.Writer) {
	fmt.Fprint(w, strconv.Quote(e.String()))
}

type DAQueryScopeInput struct {
	DimensionFilters []*DADimensionFilterInput `json:"dimensionFilters"`
}

type DADimensionFilterInput struct {
	DimensionColumnName string                         `json:"dimensionColumnName"`
	AND                 []*DADimensionLevelFilterInput `json:"and"`
	OR                  []*DADimensionLevelFilterInput `json:"or"`
}

type DADimensionLevelFilterInput struct {
	DimensionLevelColumnName string               `json:"dimensionLevelColumnName"`
	CmpOperator              DAComparisonOperator `json:"cmpOperator"`
	Values                   []string             `json:"values"`
}

type DAQueryPostAggFilterInput struct {
	MeasureFilters []*DAMeasureFilterInput `json:"measureFilters"`
	AND            []*DAMeasureFilterInput `json:"and"`
	OR             []*DAMeasureFilterInput `json:"or"`
}

type DADimensionLevelSearchFilterInput struct {
	DimensionLevelColumnName string `json:"dimensionLevelColumnName"`
	Keyword                  string `json:"keyword"`
}

type DADimensionSearchFilterInput struct {
	DimensionColumnName string                               `json:"dimensionColumnName"`
	LevelFilters        []*DADimensionLevelSearchFilterInput `json:"levelFilters"`
}

type DAQuerySearchInput struct {
	DimensionFilters []*DADimensionSearchFilterInput `json:"dimensionFilters"`
}

type DAQueryResult struct {
	DimensionMembers []*DADimensionMember `json:"dimensionMembers"`
	MeasureValues    []*string            `json:"measureValues"`
}

type DAQueryResultCache struct {
	DimensionMembers []*DADimensionMember
	MeasureValues    []sql.NullString
}

type DAQueryResultConnection struct {
	Edges    []*DAQueryResultEdge `json:"edges"`
	PageInfo *DAPageInfo          `json:"pageInfo"`
	Error    string               `json:"error"`
}

type DAQueryResultEdge struct {
	Node   *DAQueryResult `json:"node"`
	Cursor string         `json:"cursor"`
}

type DAQuerySortEntryInput struct {
	ColumnName string               `json:"columnName"`
	Direction  DAQuerySortDirection `json:"direction"`
}

type DAQuerySortInput struct {
	Entries []*DAQuerySortEntryInput `json:"entries"`
}

type DAQueryPostAggGroupingInput struct {
	GroupByColumns     []string `json:"groupByColumns"`
	AggregatedMeasures []string `json:"aggregatedMeasures"`
}

type DAQuerySortDirection string

const (
	DAQuerySortDirectionAsc  DAQuerySortDirection = "ASC"
	DAQuerySortDirectionDesc DAQuerySortDirection = "DESC"
)

var AllDAQuerySortDirection = []DAQuerySortDirection{
	DAQuerySortDirectionAsc,
	DAQuerySortDirectionDesc,
}

func (e DAQuerySortDirection) IsValid() bool {
	switch e {
	case DAQuerySortDirectionAsc, DAQuerySortDirectionDesc:
		return true
	}
	return false
}

func (e DAQuerySortDirection) String() string {
	return string(e)
}

func (e *DAQuerySortDirection) UnmarshalGQL(v interface{}) error {
	str, ok := v.(string)
	if !ok {
		return fmt.Errorf("enums must be strings")
	}

	*e = DAQuerySortDirection(str)
	if !e.IsValid() {
		return fmt.Errorf("%s is not a valid DAQuerySortDirection", str)
	}
	return nil
}

func (e DAQuerySortDirection) MarshalGQL(w io.Writer) {
	fmt.Fprint(w, strconv.Quote(e.String()))
}

type DARelationalOperator string

const (
	DARelationalOperatorEq DARelationalOperator = "EQ"
	DARelationalOperatorNe DARelationalOperator = "NE"
	DARelationalOperatorGe DARelationalOperator = "GE"
	DARelationalOperatorGt DARelationalOperator = "GT"
	DARelationalOperatorLe DARelationalOperator = "LE"
	DARelationalOperatorLt DARelationalOperator = "LT"
	DARelationalOperatorIn DARelationalOperator = "IN"
)

var AllDARelationalOperator = []DARelationalOperator{
	DARelationalOperatorEq,
	DARelationalOperatorNe,
	DARelationalOperatorGe,
	DARelationalOperatorGt,
	DARelationalOperatorLe,
	DARelationalOperatorLt,
	DARelationalOperatorIn,
}

func (e DARelationalOperator) IsValid() bool {
	switch e {
	case DARelationalOperatorEq, DARelationalOperatorNe, DARelationalOperatorGe, DARelationalOperatorGt, DARelationalOperatorLe, DARelationalOperatorLt, DARelationalOperatorIn:
		return true
	}
	return false
}

func (e DARelationalOperator) String() string {
	return string(e)
}

func (e *DARelationalOperator) UnmarshalGQL(v interface{}) error {
	str, ok := v.(string)
	if !ok {
		return fmt.Errorf("enums must be strings")
	}

	*e = DARelationalOperator(str)
	if !e.IsValid() {
		return fmt.Errorf("%s is not a valid DARelationalOperator", str)
	}
	return nil
}

func (e DARelationalOperator) MarshalGQL(w io.Writer) {
	fmt.Fprint(w, strconv.Quote(e.String()))
}

func (e DARelationalOperator) GetOperator() string {

	switch e {
	case DARelationalOperatorEq:
		return "="
	case DARelationalOperatorNe:
		return "!="
	case DARelationalOperatorGe:
		return ">="
	case DARelationalOperatorGt:
		return ">"
	case DARelationalOperatorLe:
		return "<="
	case DARelationalOperatorLt:
		return "<"
	case DARelationalOperatorIn:
		return "IN"
	}
	return ""
}
