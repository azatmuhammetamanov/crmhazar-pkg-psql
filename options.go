package crmhazar_pkg_psql

type Options struct {
	Host          string
	Port          string
	Database      string
	Username      string
	Password      string
	PgPoolMaxConn int
}
