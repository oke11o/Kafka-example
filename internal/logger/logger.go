package logger

import (
	"fmt"
	"io"
	"os"

	"github.com/rs/zerolog"
)

const (
	FormatJSON    = "json"
	FormatConsole = "console"

	envLogFormat = "APP_LOG_FORMAT"
)

// New creates a new logger instance based on APP_LOG_FORMAT environment variable
// Supported formats:
// - json (default)
// - console
func New() zerolog.Logger {
	format := os.Getenv(envLogFormat)
	if format == "" {
		format = FormatJSON
	}

	var writer io.Writer = os.Stdout

	switch format {
	case FormatJSON:
		// default writer is already os.Stdout
	case FormatConsole:
		writer = zerolog.ConsoleWriter{
			Out:        os.Stdout,
			TimeFormat: "2006-01-02 15:04:05",
		}
	default:
		fmt.Printf("Unknown log format: %s, using JSON\n", format)
	}

	return zerolog.New(writer).With().Timestamp().Logger()
}
