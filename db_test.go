package dal

import (
	"database/sql"
	"fmt"
	m "roamer-backend/api/models"
	"roamer-backend/api/util/models"
	"testing"
)

func scanUser(rows *sql.Rows, user *m.User) {

	e := rows.Scan(
		&user.ID,
		&user.FirstName,
		&user.LastName,
		&user.Email,
		&user.PhoneNumber,
		&user.CreatedAt,
		&user.UpdatedAt,
		&user.IsAdmin,
	)

	if e != nil {
		fmt.Errorf("ERROR: %s\n", e.Error())
	}

}

func TestDb(t *testing.T) {
	config := models.Config{}
	config.DBName = "roamer"
	config.DBHost = "127.0.0.1"
	config.DBUser = "root"
	config.DBPass = "blobert"
	NewDal(config)
	DB.Schema.AddTable("user", []string{
		"id",
		"firstName",
		"lastName",
		"email",
		"phoneNumber",
		"createdAt",
		"updatedAt",
		"isAdmin",
	})

	var e error
	var rows *sql.Rows
	sql := DB.Schema.Select("user").SQL()
	fmt.Printf("#### SQL: %s\n", sql)
	rows, e = DB.Connection.Query(sql)

	if e != nil {
		fmt.Printf("ERROR: %s\n", e.Error())
		return
	}

	users := []m.User{}
	for rows.Next() {
		user := new(m.User)
		e = rows.Scan(
			&user.ID,
			&user.FirstName,
			&user.LastName,
			&user.Email,
			&user.PhoneNumber,
			&user.CreatedAt,
			&user.UpdatedAt,
			&user.IsAdmin,
		)

		if e != nil {
			t.Errorf("ERROR: %s\n", e.Error())
			return
		}
		users = append(users, *user)
	}

	for _, u := range users {
		fmt.Printf("User: %s\n", u.Email)
	}
	fmt.Printf("SQL: %s\n", sql)
}
