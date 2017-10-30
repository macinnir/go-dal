package dal

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"
)

// Query defines a query to be made against the database
type Query struct {
	Table        *Table
	Filters      []ValueField
	Params       []string
	Joins        []Join
	resultLimit  int
	resultOffset int
	GroupBy      string
	SetFields    []string
	QueryType    string
	ValueFields  []ValueField
	Dal          *Dal
	Values       []interface{}
}

//SQL returns the raw sql string
func (q *Query) SQL() string {

	var sql string

	switch q.QueryType {
	case "select":
		sql = q.Select()
	case "update":
		sql = q.Update()
	case "delete":
		sql = q.Delete()
	case "insert":
		sql = q.Insert()
	default:
		panic("Unknown query type")
	}

	// fmt.Printf("SQL: %s\n", sql)

	return sql
}

// Query runs the query
func (q *Query) Query() (*sql.Rows, error) {
	query := q.SQL()
	// for _, filter := range q.Filters
	return q.Dal.Connection.Query(query, q.Values...)
}

// Exec executes a sql statement
func (q *Query) Exec() (result sql.Result, e error) {
	query := q.SQL()
	var stmt *sql.Stmt
	stmt, e = q.Dal.Connection.Prepare(query)
	if e != nil {
		return
	}

	result, e = stmt.Exec(q.Values...)
	return
}

// And adds an and conjuction in the where clause
func (q *Query) And() *Query {

	q.Where("#and#", "")
	return q
}

// Or adds an or conjuction in the where clause
func (q *Query) Or() *Query {
	q.Where("#or#", "")
	return q
}

// Where adds a filter on to the where clause
func (q *Query) Where(name string, value interface{}) *Query {

	q.Filters = append(q.Filters, ValueField{
		Name:  name,
		Value: value,
	})

	return q
}

// Insert builds an insert statement
func (q *Query) Insert() string {

	if len(q.ValueFields) > 0 {
		fields := []string{}
		values := []string{}
		for _, updateField := range q.ValueFields {

			fields = append(fields, "`"+updateField.Name+"`")
			// value := parseValue(updateField.Value)
			q.Values = append(q.Values, updateField.Value)
			values = append(values, "?")
		}
		return fmt.Sprintf("INSERT INTO `%s` (%s) VALUES (%s)", q.Table.Name, strings.Join(fields, ","), strings.Join(values, ","))
	}

	panic("No fields to update")
}

// Set sets a value for an insert or update statement
func (q *Query) Set(fieldName string, value interface{}) *Query {

	q.ValueFields = append(q.ValueFields, ValueField{
		Name:  fieldName,
		Value: value,
	})
	return q
}

// Update builds an update statement
func (q *Query) Update() string {

	where := q.buildWhere()
	setFieldStrings := []string{}

	if len(q.ValueFields) > 0 {

		for _, updateField := range q.ValueFields {

			q.Values = append(q.Values, updateField.Value)

			setFieldStrings = append(setFieldStrings, fmt.Sprintf("`%s`.`%s` = ?", q.Table.Alias, updateField.Name))
		}
	}

	return fmt.Sprintf("UPDATE `%s` `%s` SET %s WHERE %s", q.Table.Name, q.Table.Alias, strings.Join(setFieldStrings, ", "), where)
}

// Delete builds a delete query
func (q *Query) Delete() string {

	where := q.buildWhere()

	return fmt.Sprintf("DELETE FROM `%s` USING `%s` AS `%s` WHERE %s", q.Table.Alias, q.Table.Name, q.Table.Alias, where)
}

// Select builds a select statement
func (q *Query) Select() string {

	query := "SELECT "
	colStrings := []string{}
	for _, fk := range q.Table.FieldKeys {
		f := q.Table.Fields[fk]
		colStrings = append(colStrings, fmt.Sprintf("`%s`.`%s`", q.Table.Alias, f.Name))
	}

	query = query + strings.Join(colStrings, ", ") + " \nFROM `" + q.Table.Name + "` `" + q.Table.Alias + "`"

	if len(q.Joins) > 0 {

		joinStrings := ""

		for _, j := range q.Joins {

			onFields := []string{}

			for _, f := range j.Fields {

				onField, eq := q.parseFilterName(f.Name)

				onField = fmt.Sprintf("`%s`.`%s` %s ?", j.Table.Alias, onField, eq)
				q.Values = append(q.Values, f.Value)
				// switch f.Value.(type) {
				// case int:
				// 	onField = fmt.Sprintf("`%s`.`%s` %s %d", j.Table.Alias, onField, eq, f.Value.(int))
				// case int64:
				// 	onField = fmt.Sprintf("`%s`.`%s` %s %d", j.Table.Alias, onField, eq, f.Value.(int64))
				// case float64:
				// 	onField = fmt.Sprintf("`%s`.`%s` %s %f", j.Table.Alias, onField, eq, f.Value.(float64))
				// case string:
				// 	val := f.Value.(string)
				// 	if _, ok := q.Table.Fields[val]; ok {
				// 		onField = fmt.Sprintf("`%s`.`%s` %s `%s`.`%s`", j.Table.Alias, onField, eq, q.Table.Alias, val)
				// 	} else {
				// 		onField = fmt.Sprintf("`%s`.`%s` %s '%s'", j.Table.Alias, onField, eq, val)
				// 	}
				// default:
				// 	panic(fmt.Sprintf("Unknown type for field %s.%v", j.Table.Name, f.Value))
				// }
				onFields = append(onFields, onField)
			}

			joinString := fmt.Sprintf("JOIN `%s` `%s` ON %s", j.Table.Name, j.Table.Alias, strings.Join(onFields, " AND "))
			joinStrings = joinStrings + "\n" + joinString
		}

		query = query + "\n" + joinStrings
	}

	where := q.buildWhere()

	if len(where) > 0 {
		query = query + " \nWHERE " + where
	}

	if len(q.GroupBy) > 0 {
		query = query + " GROUP BY " + q.GroupBy
	}

	if q.resultLimit > 0 {
		query = query + " LIMIT " + strconv.Itoa(q.resultLimit)
	}

	if q.resultOffset > 0 {
		query = query + " OFFSET " + strconv.Itoa(q.resultOffset)
	}

	return query
}

func parseValue(field interface{}) string {

	var value string

	switch field.(type) {
	case int:
		value = strconv.Itoa(field.(int))
	case int64:
		value = strconv.FormatInt(field.(int64), 10)
	case float64:
		value = strconv.FormatFloat(field.(float64), 'E', -1, 64)
	case string:
		value = "'" + field.(string) + "'"
	default:
		panic(fmt.Sprintf("Unknown type for value: %v", field))
	}

	return value
}

// Join adds a join clause on to a select statement
func (q *Query) Join(tableName string) *Query {

	var joinTable *Table
	var ok bool

	if joinTable, ok = q.Dal.Schema.Tables[tableName]; !ok {
		panic("Invaild table name used in join clause")
	}

	q.Joins = append(q.Joins, Join{
		Table:  joinTable,
		Fields: []ValueField{},
	})

	return q
}

// On adds an on clause to the most recent join
func (q *Query) On(fieldName string, value interface{}) *Query {

	numJoins := len(q.Joins)
	// Get the most recent joins
	if numJoins == 0 {
		panic("Cannot call `On` method before any Joins have been created")
	}

	latestJoinIdx := numJoins - 1

	q.Joins[latestJoinIdx].Fields = append(q.Joins[latestJoinIdx].Fields, ValueField{
		fieldName,
		value,
	})

	return q
}

// Limit adds a limit clause to the query
func (q *Query) Limit(limit int) *Query {
	q.resultLimit = limit
	return q
}

// Offset adds an offset clause to the query
func (q *Query) Offset(offset int) *Query {
	q.resultOffset = offset
	return q
}

func (q *Query) parseFilterName(filterName string) (realFilterName string, eq string) {

	eq = "="
	realFilterName = filterName

	filterStartChar := filterName[0:1]

	if filterStartChar == "!" {
		eq = "!="
		realFilterName = filterName[1:]
		return
	}

	if filterStartChar == ">" || filterStartChar == "<" {
		eq = filterStartChar
		realFilterName = filterName[1:]
		return
	}

	if filterName[0:2] == ">=" || filterName[0:2] == "<=" {
		eq = filterName[0:2]
		realFilterName = filterName[2:]
	}

	return
}

func (q *Query) buildWhere() string {

	var eq string
	where := ""
	whereClauses := []string{}
	filterLen := len(q.Filters)
	filterIdx := 0

	if len(q.Filters) > 0 {

		for _, filter := range q.Filters {

			if filter.Name == "#and#" || filter.Name == "#or#" {
				whereClauses = append(whereClauses, strings.ToUpper(filter.Name[1:len(filter.Name)-1]))
				filterIdx = filterIdx + 1
				continue
			}

			filter.Name, eq = q.parseFilterName(filter.Name)

			// value := parseValue(filter.Value)
			q.Values = append(q.Values, filter.Value)
			whereClauses = append(whereClauses, fmt.Sprintf("`%s`.`%s` %s ?", q.Table.Alias, filter.Name, eq))
			filterIdx = filterIdx + 1
			if filterIdx < filterLen {
				if q.Filters[filterIdx].Name[0:1] != "#" {
					whereClauses = append(whereClauses, "AND")
				}
			}
		}
	}

	if len(whereClauses) > 0 {
		where = strings.Join(whereClauses, " ")
	}

	return where
}
