package dal

import (
	"fmt"
	"testing"
)

func TestSchema(t *testing.T) {

	schemaName := "foo"
	s := NewSchema(schemaName)

	if s.Name != schemaName {
		t.Errorf("Schema name should have been '%s'", schemaName)
	}

	s.AddTable("foo", []string{})
	if s.Table("foo").Alias != "f" {
		t.Errorf("Table alias should have been '%s'", "f")
	}
	s.Table("foo").AddField("id")
	s.Table("foo").AddField("name")
	if s.Table("foo").Field("id").Name != "id" {
		t.Errorf("Table foe should have a column name id but doesn't")
	}

	s.AddTable("foe", []string{
		"id",
		"name",
		"createdAt",
		"updatedAt",
		"fooId",
	})
	if s.Table("foe").Alias != "f1" {
		t.Errorf("Table alias should have been '%s'", "f1")
	}

	selectString := s.Select("foe").Join("foo").On("fooId", "id").Where("name", "foo").Where("!fooId", 123).Or().Where("foodId", 111).SQL()
	fmt.Printf("SELECT: %s\n", selectString)

	// updateString :=
	s.Update("foe").Set("name", "Another name").Set("fooId", 123).Where("id", 123).SQL()
	// fmt.Printf("UPDATE: %s\n", updateString)

	// deleteString :=
	s.Delete("foe").Where("id", 1).SQL()
	// fmt.Printf("DELETE: %s\n", deleteString)

	// insertString :=
	s.Insert("foe").Set("name", "foo").Set("fooId", 1).SQL()
	// fmt.Printf("INSERT: %s\n", insertString)
	// q.Set
	// q.Update()
}
