# Serverless Redis

## Basic behaviour
* Launch containers for Redis DBs (onto the same host)
* If a container hasn't had any requests after a timeout - say 5 miuntes, then save the state of the container (a snapshot of the DB), and then save this to disk (can be expanded to do this on cloud storage)
* Make new DBs, recieve a request to make a DB, then get returned a handle to access this DB
* Routing: One server manages all the requests to the different DBs

## Components

### HTTP Server

POST /database
* Creates a new database, returns credentials to access the DB

DELETE /database/{databaseID}
* Deletes a database

GET /database/{databaseID}
* Returns the information of a current DB. I.e. whether it is currently active, lastRequestTime etc


### TCP Proxy
* Given an Auth token for a DB (for now we can just set this as the DB ID to not over complicate things), proxy request to the DB. 
* Uses the Manager to ensure that the DB requested exists and is active
* Not quite sure how to do this, but its possible, will figure it out completely when the time comes
* Each request also needs to update the time of the last request sent to the DB

### DBManager 
* Manages which DBs are currently active/inactive, along with pausing any idle databases
* Has a DBRuntime object which is used to manage the lifecycle of DBs
* In the future, we can move to storing active/inactive database connections in a server - so it can survice between restarts/crashes, and know which Redis Databases are currently active
* For now, this is the only object that directly changes the RedisDB objects

#### Fields
* ActiveDBs: Map[databaseID: RedisDB]. Active DBs
* InActiveDBs: Map[databaseID: RedisDB]. Inactive DBs (but not deleted). Since the numebr of inactive DBs is likely to be far larger than the number of activeDBs, storing these explicitly (instead of in one list), allows you to iterate only over the active ones easily
* active: bool. Whether the manager is currently active
* databaseIDleLimit: int. How long, in seconds, databases last before being paused without any requests
* dbRuntime: DBRuntime: runtime currently being used for DBs

#### Methods:
new(runtime: DBRuntime)
* Creates a new manager

start():
* Starts the manager. Sets active = true
* Starts runDatabaseReaper in the background

stop()
* Stops the manager. Sets active = false
* Stops the ongoing DB reaper process.
* Deletes all active DBs

ensureActive(databaseID: str): str
* Uses the runtime to ensure that the DB is active
* Then returns the allocated URL from the runtime

createDB(): ID
* Figures out a new database ID (say a long alphanumeric code)
* Uses the runtime to create a DB, returns the created ID (this is used for the proxy)
* Adds this DB to the active list

pauseDB(databaseID: str): bool
* Uses the runtime to pause a DB
* Sets the url to be None
* Then moves the DB to the inactive list

deleteDB(databaseID: str): bool
* Uses the runtime to delete the DB
* Removes the DB from both the active and inactive lists

runDatabaseReaper(): None
* Every 10 seconds, iterates through databases, and pauses any that haven't had any requests beyond the timeout limit

recordActivity(databaseID: str): bool
* Updates the latest request time for a DB, used by the proxy

### DBRuntime
* Handles creating DBs to their specific runtime (e.g. on the cloud, or local docker containers)

#### Methods:
createDB(databaseID: str): url
* Creates a new DB with the given ID. Starts the DB, then returns the created url
* Throw an error if a database can't be created (e.g. run-out of ports to allocate to)

resumeDB(databaseID: str): str
* Resumes a DB, then returns the new url

pauseDB(databaseID: str): bool
* Pauses a DB, and saves it's state to disk
* Sets the URL of the DB to None

deleteDB(databaseID: str): bool
* Deletes a DB, and any of it's saved state

### LocalDBRuntime
* Manages DBs that exist locally. We assume that snapshots are saved to the local file system
* Local Docker containers can just use volumes to manage their state - you then get a very natural resuming+pausing process.

#### Fields:
portPool: int GO channel that gives a port to be allocateed to. Claude gave this implementation to work off:

```go
type PortPool chan int

func NewPortPool(from, to int) PortPool {
	p := make(PortPool, to-from+1)
	for port := from; port <= to; port++ {
		p <- port
	}
	return p
}

func (p PortPool) Acquire() int     { return <-p } // Needs to return an error if there is no port available (apparently this would otherwise block forever)
func (p PortPool) Release(port int) { p <- port }
```

#### Methods:
* As described above, but use local docker containers and file system to manage DBs and state respectively.

### RedisDB
* Represents a DB

#### Fields
* lastRequest: DateTime: last time this DB was accessed
* active: bool: Whether a DB is active or not
* url: str. The URL to the DB (can be set to localhost:PORT for now). None if inactive

## Extensions
* Concurrency protection for pausing and resuming DBs - this is is an edge case needed so that two clients don't try to resume the same DB at once
* Add support for users
* Add auth (essential for if we are deploying)
