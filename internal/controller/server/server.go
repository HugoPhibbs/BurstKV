package server

import (
	"burstkv/internal/controller"
	"burstkv/internal/controller/domain/database"
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"
)

func New(ctrl *controller.Controller) *gin.Engine {
	router := gin.Default()

	router.GET("/database/list", func(c *gin.Context) {
		databases := ctrl.GetAllDatabaseIDs()
		if databases == nil {
			databases = []string{}
		}
		c.JSON(http.StatusOK, gin.H{"databases": databases})
	})

	router.GET("/database/:databaseId", func(c *gin.Context) {
		state, err := ctrl.GetDatabaseState(c.Param("databaseId"))
		if err != nil {
			writeError(c, err)
			return
		}
		if state == database.NotFound {
			writeError(c, controller.ErrDatabaseNotFound)
			return
		}
		c.JSON(http.StatusOK, gin.H{"status": state.String()})
	})

	router.POST("/database", func(c *gin.Context) {
		databaseId, err := ctrl.CreateDatabase(c.Request.Context())
		if err != nil {
			writeError(c, err)
			return
		}
		c.JSON(http.StatusOK, gin.H{"databaseId": databaseId})
	})

	router.DELETE("/database/delete/:databaseId", func(c *gin.Context) {
		writeResult(c, ctrl.DeleteDatabase(c.Request.Context(), c.Param("databaseId")))
	})

	router.POST("/database/pause/:databaseId", func(c *gin.Context) {
		writeResult(c, ctrl.PauseDatabase(c.Request.Context(), c.Param("databaseId")))
	})

	router.POST("/database/resume/:databaseId", func(c *gin.Context) {
		writeResult(c, ctrl.ResumeDatabase(c.Request.Context(), c.Param("databaseId")))
	})

	return router
}

// Writes a success response, or the error if there is one.
func writeResult(c *gin.Context, err error) {
	if err != nil {
		writeError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"success": true})
}

// Maps controller errors to HTTP status codes.
func writeError(c *gin.Context, err error) {
	code := http.StatusInternalServerError
	switch {
	case errors.Is(err, controller.ErrDatabaseNotFound):
		code = http.StatusNotFound
	case errors.Is(err, controller.ErrNoAvailableWorkers):
		code = http.StatusServiceUnavailable
	}
	c.JSON(code, gin.H{"error": err.Error()})
}
