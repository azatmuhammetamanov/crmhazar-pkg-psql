package test

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"gitlab.com/salamtm.messenger/spsql"
	"log"
	"os"
	"testing"
)

var options = spsql.Options{
	Host:          "localhost",
	Port:          "5432",
	Database:      "test_db",
	Username:      "test_user",
	Password:      "test_password",
	PgPoolMaxConn: 400,
}

func TestMain(m *testing.M) {
	testContainer := SetupTestContainer(&options)

	code := m.Run()
	teardown(testContainer)
	os.Exit(code)
}

func teardown(testContainer *TestContainer) {
	testContainer.TearDown()
}

func TestNewClient(t *testing.T) {

	ctx := context.Background()

	// psql client
	psqlClient, err := spsql.NewClient(ctx, options)

	if assert.NoError(t, err) {
		log.Println("✅ psql connected")
	}

	defer psqlClient.Close()

	// create table
	t.Run("create table", func(t *testing.T) {
		err = createTable(ctx, psqlClient)
		assert.NoError(t, err)
	})

	insertValueColumn1 := "value_column_1"
	insertValueColumn2 := "value_column_2"
	insertedId := 0

	// insertRow
	t.Run("insertRow", func(t *testing.T) {
		id, err := insertRow(ctx, psqlClient, insertValueColumn1, insertValueColumn2)

		assert.NoError(t, err)
		assert.NotEqual(t, insertedId, id)

		insertedId = id
	})

	// select
	t.Run("select inserted row", func(t *testing.T) {
		resultValueColumn1, resultValueColumn2, err := selectRow(ctx, psqlClient, insertedId)

		assert.NoError(t, err)

		if assert.Equal(t, insertValueColumn1, resultValueColumn1) && assert.Equal(t, insertValueColumn2, resultValueColumn2) {
			log.Println("✅ selected column equal with inserted column")
		}

	})

	updateValueColumn1 := "update_value_column_1"
	updateValueColumn2 := "update_value_column_2"

	// updateRow
	t.Run("updateRow", func(t *testing.T) {
		err = updateRow(ctx, psqlClient, insertedId, updateValueColumn1, updateValueColumn2)
		assert.NoError(t, err)
	})

	// select
	t.Run("select updated row", func(t *testing.T) {
		resultValueColumn1, resultValueColumn2, err := selectRow(ctx, psqlClient, insertedId)

		assert.NoError(t, err)

		if assert.Equal(t, updateValueColumn1, resultValueColumn1) && assert.Equal(t, updateValueColumn2, resultValueColumn2) {
			log.Println("✅ selected column equal with updated column")
		}
	})

	// delete
	t.Run("delete", func(t *testing.T) {
		err = deleteRow(ctx, psqlClient, insertedId)
		assert.NoError(t, err)
	})

	// select
	t.Run("select deleted row", func(t *testing.T) {
		count, err := selectCountRows(ctx, psqlClient, insertedId)

		assert.NoError(t, err)

		if assert.Equal(t, 0, count) {
			log.Println("✅ selected columns row count = 0")
		}
	})

	// drop table
	t.Run("drop table", func(t *testing.T) {
		err = dropTable(ctx, psqlClient)
		assert.NoError(t, err)
	})

}

func createTable(ctx context.Context, psqlClient spsql.Client) error {
	b, err := os.ReadFile("up.sql")
	if err != nil {
		log.Println("can not read up.sql file ", err)
		return err
	}

	sql := string(b)

	log.Println("🔔 sql: ", sql)

	commandTag, err := psqlClient.Exec(ctx, sql)
	if err != nil {
		log.Println("🚫 table can not create err: ", err)
		return err
	}

	log.Println("✅ table created success, commandTag: ", commandTag)

	return nil
}

func insertRow(ctx context.Context, psqlClient spsql.Client, insertValueColumn1 string, insertValueColumn2 string) (int, error) {
	id := 0

	sql := `INSERT INTO test(column_1, column_2) VALUES (@insert_column_1, @insert_column_2) RETURNING id;`
	args := pgx.NamedArgs{
		"insert_column_1": insertValueColumn1,
		"insert_column_2": insertValueColumn2,
	}

	log.Println("🔔 sql: ", sql)
	log.Println("🔔 args: ", args)

	err := psqlClient.QueryRow(ctx, sql, args).Scan(&id)
	if err != nil {
		return id, fmt.Errorf("🚫 unable to insertRow row: %w", err)
	}

	log.Println("✅ data inserted success, id: ", id)

	return id, nil
}

func selectRow(ctx context.Context, psqlClient spsql.Client, id int) (string, string, error) {

	resultValueColumn1 := ""
	resultValueColumn2 := ""

	sql := `SELECT column_1, column_2 FROM test WHERE id = @id`
	args := pgx.NamedArgs{
		"id": id,
	}

	log.Println("🔔 sql: ", sql)
	log.Println("🔔 args: ", args)

	err := psqlClient.QueryRow(ctx, sql, args).Scan(&resultValueColumn1, &resultValueColumn2)
	if err != nil {
		return resultValueColumn1, resultValueColumn2, fmt.Errorf("🚫 unable to select row: %w", err)
	}

	log.Println("✅ data selected success")
	log.Println("🔔 resultValueColumn1: ", resultValueColumn1)
	log.Println("🔔 resultValueColumn2: ", resultValueColumn2)

	return resultValueColumn1, resultValueColumn2, nil
}

func updateRow(ctx context.Context, psqlClient spsql.Client, id int, updateValueColumn1 string, updateValueColumn2 string) error {
	sql := `UPDATE test SET column_1 = @update_column_1,  column_2 = @update_column_2 WHERE id = @id`
	args := pgx.NamedArgs{
		"id":              id,
		"update_column_1": updateValueColumn1,
		"update_column_2": updateValueColumn2,
	}

	log.Println("🔔 sql: ", sql)
	log.Println("🔔 args: ", args)

	commandTag, err := psqlClient.Exec(ctx, sql, args)
	if err != nil {
		return fmt.Errorf("🚫 unable to updateRow row: %w", err)
	}

	log.Println("✅ data updated success, commandTag: ", commandTag)

	return nil
}

func deleteRow(ctx context.Context, psqlClient spsql.Client, id int) error {
	sql := `DELETE FROM test WHERE id = @id`
	args := pgx.NamedArgs{
		"id": id,
	}

	log.Println("🔔 sql: ", sql)
	log.Println("🔔 args: ", args)

	commandTag, err := psqlClient.Exec(ctx, sql, args)
	if err != nil {
		return fmt.Errorf("🚫 unable to delete row: %w", err)
	}

	log.Println("✅ data deleted success, commandTag: ", commandTag)

	return nil
}

func selectCountRows(ctx context.Context, psqlClient spsql.Client, id int) (int, error) {
	count := 0

	sql := `SELECT count(id) as count FROM test WHERE id = @id`
	args := pgx.NamedArgs{
		"id": id,
	}

	log.Println("🔔 sql: ", sql)
	log.Println("🔔 args: ", args)

	err := psqlClient.QueryRow(ctx, sql, args).Scan(&count)
	if err != nil {
		return count, fmt.Errorf("🚫 unable to select row: %w", err)
	}

	log.Println("✅ data selected success")
	log.Println("🔔 count: ", count)

	return count, nil
}

func dropTable(ctx context.Context, psqlClient spsql.Client) error {
	b, err := os.ReadFile("down.sql")
	if err != nil {
		log.Println("can not read down.sql file ", err)
		return err
	}

	sql := string(b)

	log.Println("🔔 sql: ", sql)

	commandTag, err := psqlClient.Exec(ctx, sql)
	if err != nil {
		log.Println("🚫 drop table err: ", err)
		return err
	}

	log.Println("✅ table dropped success, commandTag: ", commandTag)

	return nil
}
