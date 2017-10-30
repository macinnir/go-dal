package dal

// ValueField is a name/value pair used to setting data on insert or update queries
type ValueField struct {
	Name  string
	Value interface{}
}
