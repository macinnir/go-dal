package dal

import (
	"fmt"
	"strconv"
)

// NewSchema defines a new schema
func NewSchema(dal *Dal, name string) *Schema {

	s := new(Schema)
	s.Name = name
	s.Tables = map[string]*Table{}
	s.Aliases = map[string]string{}
	s.Dal = dal
	return s
}

// Schema is a collection of tables
type Schema struct {
	Name    string
	Tables  map[string]*Table
	Aliases map[string]string
	Dal     *Dal
}

// AddTable adds a table to the schema
func (s *Schema) AddTable(name string, fields []string) error {
	t := NewTable(name)
	if len(fields) > 0 {
		t.AddFields(fields)
	}
	return s.define(t)
}

// Table gets a table
func (s *Schema) Table(name string) (t *Table) {

	var ok bool

	if t, ok = s.Tables[name]; !ok {
		panic(fmt.Sprintf("No table named `%s` has been defined", name))
	}

	return
}

// Select starts a select statement
func (s *Schema) Select(tableName string) *Query {
	return s.newQuery(tableName, "select")
}

// Update starts an update query
func (s *Schema) Update(tableName string) *Query {
	return s.newQuery(tableName, "update")
}

// Delete starts a delete query
func (s *Schema) Delete(tableName string) *Query {
	return s.newQuery(tableName, "delete")
}

// Insert starts an insert query
func (s *Schema) Insert(tableName string) *Query {
	return s.newQuery(tableName, "insert")
}

func (s *Schema) newQuery(tableName string, queryType string) *Query {
	q := new(Query)
	q.Dal = s.Dal
	q.Table = s.Table(tableName)
	q.QueryType = queryType
	return q
}

// define adds a table to the set of tables in the schema
func (s *Schema) define(t *Table) (e error) {

	var ok bool

	if _, ok = s.Tables[t.Name]; ok {
		e = fmt.Errorf("a table named `%s` has already been defined", t.Name)
		return
	}

	alias := t.Name[0:1]
	tryAlias := alias
	tries := 0
	for {
		if _, ok = s.Aliases[tryAlias]; !ok {
			alias = tryAlias
			break
		}

		tries = tries + 1
		tryAlias = alias + strconv.Itoa(tries)
	}

	t.Alias = alias
	s.Tables[t.Name] = t
	s.Aliases[alias] = t.Name
	return
}
