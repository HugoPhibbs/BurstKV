package server

import (
	"burstkv/internal/controller"
	"burstkv/internal/core"
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"
)

func New(ctrl *controller.Controller) *gin.Engine {
	router := gin.Default()

	router.GET("/database", func(c *gin.Context) {
		statuses, err := ctrl.GetAllDatabaseStatuses(c.Request.Context())
		if err != nil {
			writeError(c, err)
			return
		}
		c.JSON(http.StatusOK, gin.H{"statuses": statuses})
	})

	router.GET("/database/:databaseID", func(c *gin.Context) {
		status, err := ctrl.GetDatabaseStatus(c.Request.Context(), c.Param("databaseID"))
		if err != nil {
			writeError(c, err)
			return
		}
		if status == nil {
			writeError(c, core.ErrDatabaseNotFound)
			return
		}
		c.JSON(http.StatusOK, gin.H{"status": status})
	})

	router.POST("/database", func(c *gin.Context) {
		databaseID, password, err := ctrl.CreateDatabase(c.Request.Context())
		if err != nil {
			writeError(c, err)
			return
		}
		c.JSON(http.StatusOK, gin.H{"databaseID": databaseID, "password": password})
	})

	router.DELETE("/database/delete/:databaseID", func(c *gin.Context) {
		writeResult(c, ctrl.DeleteDatabase(c.Request.Context(), c.Param("databaseID")))
	})

	router.POST("/database/pause/:databaseID", func(c *gin.Context) {
		writeResult(c, ctrl.PauseDatabase(c.Request.Context(), c.Param("databaseID")))
	})

	router.POST("/database/resume/:databaseID", func(c *gin.Context) {
		writeResult(c, ctrl.ResumeDatabase(c.Request.Context(), c.Param("databaseID")))
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
	_  = c.Error(err) // Attach the server error to the response

	code := http.StatusInternalServerError
	switch {
	case errors.Is(err, core.ErrDatabaseNotFound):
		code = http.StatusNotFound
	case errors.Is(err, core.ErrNoAvailablePods):
		code = http.StatusServiceUnavailable
	}
	c.JSON(code, gin.H{"error": err.Error()})
}
