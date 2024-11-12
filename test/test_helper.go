package test

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/docker/go-connections/nat"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"gitlab.com/salamtm.messenger/spsql"
)

//const (
//	DbName = "test_db"
//	DbUser = "test_user"
//	DbPass = "test_password"
//)

type TestContainer struct {
	container testcontainers.Container
}

func SetupTestContainer(options *spsql.Options) *TestContainer {

	// setup db container
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*60)
	container, err := createContainer(ctx, options)
	if err != nil {
		log.Fatal("failed to setup test ", err)
	}

	// migrate db schema
	//err = migrateDb(dbAddr)
	//if err != nil {
	//	log.Fatal("failed to perform db migration", err)
	//}
	cancel()

	return &TestContainer{
		container: container,
	}
}

func (tdb *TestContainer) TearDown() {
	log.Println("teardown ...")
	//tdb.DbInstance.Close()
	// remove test container
	err := tdb.container.Terminate(context.Background())

	if err != nil {
		log.Println("TearDown ", err)
		return
	}

	log.Println("TearDown success")
}

func createContainer(ctx context.Context, options *spsql.Options) (testcontainers.Container, error) {

	var env = map[string]string{
		"POSTGRES_PASSWORD": options.Password,
		"POSTGRES_USER":     options.Username,
		"POSTGRES_DB":       options.Database,
	}
	var port = options.Port + "/tcp"

	req := testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "postgres:14-alpine",
			ExposedPorts: []string{port},
			Env:          env,
			WaitingFor:   wait.ForLog("database system is ready to accept connections"),
		},
		Started: true,
	}

	container, err := testcontainers.GenericContainer(ctx, req)
	if err != nil {
		return container, fmt.Errorf("failed to start container: %v", err)
	}

	if err != nil {
		return container, fmt.Errorf("failed to get container external port: %v", err)
	}

	p, err := container.MappedPort(ctx, nat.Port(options.Port))
	if err != nil {
		return container, fmt.Errorf("failed to get container external port: %v", err)
	}

	host, err := container.Host(ctx)
	if err != nil {
		return container, fmt.Errorf("failed to get container external ip: %v", err)
	}

	options.Port = p.Port()

	ip, err := container.ContainerIP(ctx)
	if err != nil {
		return container, fmt.Errorf("failed to get container external ip: %v", err)
	}

	options.Host = host

	log.Println("✅ postgresql container ready and running")
	log.Println("🔔 container ip: " + ip)
	log.Println("🔔 container host: " + options.Host)
	log.Println("🔔 container port: " + options.Port)
	log.Println("🔔 container postgresql user_name: " + options.Username)
	log.Println("🔔 container postgresql password: " + options.Password)
	log.Println("🔔 container postgresql database: " + options.Database)

	time.Sleep(time.Second)

	return container, nil
}

//func migrateDb(dbAddr string) error {
//
//	// get location of test
//	_, path, _, ok := runtime.Caller(0)
//	if !ok {
//		return fmt.Errorf("failed to get path")
//	}
//	pathToMigrationFiles := filepath.Dir(path) + "/migration"
//
//	databaseURL := fmt.Sprintf("postgres://%s:%s@%s/%s?sslmode=disable", DbUser, DbPass, dbAddr, DbName)
//	m, err := migrate.New(fmt.Sprintf("file:%s", pathToMigrationFiles), databaseURL)
//	if err != nil {
//		return err
//	}
//	defer m.Close()
//
//	err = m.Up()
//	if err != nil && !errors.Is(err, migrate.ErrNoChange) {
//		return err
//	}
//
//	log.Println("migration done")
//
//	return nil
//}
