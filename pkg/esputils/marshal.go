package esputils

import (
	"database/sql"

	"github.com/99designs/gqlgen/graphql"
)

func UnmarshalNullString(v interface{}) (sql.NullString, error) {
	if v == nil {
		return sql.NullString{Valid: false}, nil
	}
	s, err := graphql.UnmarshalString(v)
	return sql.NullString{String: s, Valid: err == nil}, err
}

func MarshalNullString(ns sql.NullString) graphql.Marshaler {
	if !ns.Valid {
		return graphql.Null
	}
	return graphql.MarshalString(ns.String)
}

func UnmarshalNullInt64(v interface{}) (sql.NullInt64, error) {
	if v == nil {
		return sql.NullInt64{Valid: false}, nil
	}
	i, err := graphql.UnmarshalInt64(v)
	return sql.NullInt64{Int64: i, Valid: err == nil}, err
}

func MarshalNullInt64(ns sql.NullInt64) graphql.Marshaler {
	if !ns.Valid {
		return graphql.Null
	}
	return graphql.MarshalInt64(ns.Int64)
}

func UnmarshalNullInt32(v interface{}) (sql.NullInt32, error) {
	if v == nil {
		return sql.NullInt32{Valid: false}, nil
	}
	i, err := graphql.UnmarshalInt32(v)
	return sql.NullInt32{Int32: i, Valid: err == nil}, err
}

func MarshalNullInt32(ns sql.NullInt32) graphql.Marshaler {
	if !ns.Valid {
		return graphql.Null
	}
	return graphql.MarshalInt32(ns.Int32)
}

func UnmarshalNullTime(v interface{}) (sql.NullTime, error) {
	if v == nil {
		return sql.NullTime{Valid: false}, nil
	}
	t, err := graphql.UnmarshalTime(v)
	return sql.NullTime{Time: t, Valid: err == nil}, err
}

func MarshalNullTime(ns sql.NullTime) graphql.Marshaler {
	if !ns.Valid {
		return graphql.Null
	}
	return graphql.MarshalTime(ns.Time)
}
