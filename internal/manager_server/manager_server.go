package manager_server

import (
	"burstkv/internal/db_manager"

	"github.com/gin-gonic/gin"
)

func New(dbManager *db_manager.DBManager) *gin.Engine {
	router := gin.Default()

	router.GET("/database/status/:databaseId", func(c *gin.Context) {
		databaseId := c.Param("databaseId")
		state := dbManager.GetDatabaseState(databaseId)

		if state == db_manager.NotFound {
			c.JSON(404, gin.H{"error": "Database not found"})
			return
		}

		c.JSON(200, gin.H{"state": state.String()})
	})

	router.GET("/database/list", func(c *gin.Context) {
		databases := dbManager.GetAllDatabaseIDs()
		c.JSON(200, gin.H{"databases": databases})
	})

	router.POST("/database/create", func(c *gin.Context) {
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
