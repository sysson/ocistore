package query

import "github.com/99designs/gqlgen/graphql"

// JSON is an arbitrary JSON value exposed by manifest parsers.
type JSON any

func MarshalJSON(value JSON) graphql.Marshaler {
	return graphql.MarshalAny(value)
}

func UnmarshalJSON(value any) (JSON, error) {
	result, err := graphql.UnmarshalAny(value)
	return JSON(result), err
}
