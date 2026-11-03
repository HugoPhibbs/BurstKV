package manager_server

import (
	"burstkv/internal/db_manager"

	"github.com/gin-gonic/gin"
)

func New(dbManager *db_manager.DBManager) *gin.Engine {
	router := gin.Default()

	router.GET("/database/list", func(c *gin.Context) {
		databases := dbManager.GetAllDatabaseIDs()
		if databases == nil {
			databases = []string{}
		}
		c.JSON(200, gin.H{"databases": databases})
	})

	router.GET("/database/:databaseId", func(c *gin.Context) {
		databaseId := c.Param("databaseId")
		database, err := dbManager.GetDatabase(databaseId)
		if err != nil {
			if err == db_manager.ErrDatabaseNotFound {
				c.JSON(404, gin.H{"error": "Database not found"})
				return
			}
			c.JSON(500, gin.H{"error": err.Error()})
			return
		}

		status := dbManager.GetDatabaseState(databaseId)
		c.JSON(200, gin.H{"address": database.Address(), "status": status.String(), "lastRequest": database.LastRequestTime()})
	})

	router.POST("/database", func(c *gin.Context) {
		new_db, err := dbManager.CreateDatabase()
		if err != nil {
			c.JSON(500, gin.H{"error": err.Error()})
			return
		}
		c.JSON(200, gin.H{"address": new_db.Address(), "databaseId": new_db.Id()})
	})

	router.DELETE("/database/delete/:databaseId", func(c *gin.Context) {
		databaseId := c.Param("databaseId")
		err := dbManager.DeleteDatabase(databaseId)
		if err != nil {
			if err == db_manager.ErrDatabaseNotFound {
				c.JSON(404, gin.H{"error": "Database not found"})
				return
			}
			c.JSON(500, gin.H{"error": err.Error()})
			return
		}
		c.JSON(200, gin.H{"success": true})
	})

	router.POST("/database/pause/:databaseId", func(c *gin.Context) {
		databaseId := c.Param("databaseId")
		err := dbManager.PauseDatabase(databaseId)
		if err != nil {
			if err == db_manager.ErrDatabaseAlreadyPaused {
				c.JSON(409, gin.H{"error": "Database is already paused"})
				return
			}
			if err == db_manager.ErrDatabaseNotFound {
				c.JSON(404, gin.H{"error": "Database not found"})
				return
			}
			c.JSON(500, gin.H{"error": err.Error()})
			return
		}

		c.JSON(200, gin.H{"success": true})
	})

	router.POST("/database/resume/:databaseId", func(c *gin.Context) {
		databaseId := c.Param("databaseId")
		address, err := dbManager.ResumeDatabase(databaseId)
		if err != nil {
			if err == db_manager.ErrDatabaseAlreadyActive {
				c.JSON(409, gin.H{"error": "Database is already active"})
				return
			}
			if err == db_manager.ErrDatabaseNotFound {
				c.JSON(404, gin.H{"error": "Database not found"})
				return
			}
			c.JSON(500, gin.H{"error": err.Error()})
			return
		}
		c.JSON(200, gin.H{"address": address})
	})

	return router
}
