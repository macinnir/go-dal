package dal

import "database/sql"

// IQuery outlines the methods on build a sql query and interacting with the database
type IQuery interface {
	Query() (*sql.Rows, error)
	Exec() (result sql.Result, e error)
	And() IQuery
	Or() IQuery
	Where(name string, value interface{}) IQuery
	Set(fieldName string, value interface{}) IQuery
	Join(tableName string) IQuery
	On(tableName string, value interface{}) IQuery
	Limit(limit int) IQuery
	Offset(offset int) IQuery
	Order(field string, direction string)
}
