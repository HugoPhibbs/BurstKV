package db_runtime

type DBRuntime interface {
	CreateDatabase(databaseId string) (string, error)
	DeleteDatabase(databaseId string) error
	PauseDatabase(databaseId string) error
	ResumeDatabase(databaseId string) (string, error)
}
