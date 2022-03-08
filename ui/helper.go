package ui

import (
	"fmt"
	"github.com/contribsys/faktory/webui"
	"net/http"
)

func Relative(req *http.Request, location string) string {
	ctx := req.Context().(*webui.DefaultContext)
	return fmt.Sprintf("%s%s", ctx.Root, location)
}
