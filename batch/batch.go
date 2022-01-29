package batch

import (
	"sync"
)

type batch struct {
	Id       string
	Meta     *batchMeta
	Parents  []*batch
	Children []*batch
	mu       sync.Mutex
}

type batchMeta struct {
	Total            int
	Failed           int
	Pending          int
	Succeeded        int
	CreatedAt        string
	Description      string
	Committed        bool
	SuccessJob       string
	CompleteJob      string
	SuccessJobState  string
	CompleteJobState string
	ChildSearchDepth *int
}
