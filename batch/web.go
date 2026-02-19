package batch

import (
	"context"
	"fmt"
	"html/template"
	"net/http"
	"sort"
	"strconv"
	"strings"

	"github.com/contribsys/faktory/util"
	"github.com/contribsys/faktory/webui"
	"github.com/justinas/nosurf"
)

// batchView holds all display data for a single batch in the web UI.
type batchView struct {
	Bid           string
	Description   string
	CreatedAt     string
	Total         int64
	Pending       int64
	Failed        int64
	Succeeded     int64
	CompleteState string
	SuccessState  string
	ParentBid     string
	ChildCount    int

	// Callback definitions (job type or empty)
	CompleteType string
	SuccessType  string
}

const batchPageSize = 25

// listBatchPage uses SSCAN to paginate through batches:committed and returns
// a page of batchView results sorted by created_at descending.
func (b *BatchSubsystem) listBatchPage(ctx context.Context, cursor uint64, pageSize int) ([]batchView, uint64, int64, error) {
	rds := b.Server.Manager().Redis()

	// Total count of committed batches
	totalCount, err := rds.SCard(ctx, batchCommittedSetKey()).Result()
	if err != nil {
		return nil, 0, 0, fmt.Errorf("failed to get batch count: %w", err)
	}

	// SSCAN to get a page of BIDs
	bids, nextCursor, err := rds.SScan(ctx, batchCommittedSetKey(), cursor, "", int64(pageSize)).Result()
	if err != nil {
		return nil, 0, 0, fmt.Errorf("failed to scan batches: %w", err)
	}

	if len(bids) == 0 {
		return nil, nextCursor, totalCount, nil
	}

	views := make([]batchView, 0, len(bids))
	for _, bid := range bids {
		v, err := b.buildBatchView(ctx, bid, nil)
		if err != nil {
			util.Warnf("batch web: failed to load batch %s: %v", bid, err)
			continue
		}
		views = append(views, *v)
	}

	// Sort by created_at descending
	sort.Slice(views, func(i, j int) bool {
		return views[i].CreatedAt > views[j].CreatedAt
	})

	return views, nextCursor, totalCount, nil
}

// getBatchDetail returns the full batch view and its children's views.
func (b *BatchSubsystem) getBatchDetail(ctx context.Context, bid string) (*batchView, []batchView, error) {
	children, err := getChildBatches(ctx, b.Server, bid)
	if err != nil {
		util.Warnf("batch web: failed to get child batches for %s: %v", bid, err)
		children = nil
	}

	v, err := b.buildBatchView(ctx, bid, children)
	if err != nil {
		return nil, nil, err
	}

	if children == nil {
		return v, nil, nil
	}

	childViews := make([]batchView, 0, len(children))
	for _, childBid := range children {
		cv, err := b.buildBatchView(ctx, childBid, nil)
		if err != nil {
			util.Warnf("batch web: failed to load child batch %s: %v", childBid, err)
			continue
		}
		childViews = append(childViews, *cv)
	}

	sort.Slice(childViews, func(i, j int) bool {
		return childViews[i].CreatedAt > childViews[j].CreatedAt
	})

	return v, childViews, nil
}

// buildBatchView constructs a batchView from Redis data for a single batch.
// If children is non-nil, it is used directly for the child count instead of
// fetching from Redis, avoiding a redundant call when the caller already has
// the children list.
func (b *BatchSubsystem) buildBatchView(ctx context.Context, bid string, children []string) (*batchView, error) {
	status, err := getBatchStatus(ctx, b.Server, bid)
	if err != nil {
		return nil, err
	}

	batch, err := getBatch(ctx, b.Server, bid)
	if err != nil {
		return nil, err
	}

	if children == nil {
		fetched, err := getChildBatches(ctx, b.Server, bid)
		if err != nil {
			util.Warnf("batch web: failed to get child batches for %s: %v", bid, err)
		} else {
			children = fetched
		}
	}
	childCount := len(children)

	succeeded := status.Total - status.Pending - status.Failed
	if succeeded < 0 {
		succeeded = 0
	}

	var completeType, successType string
	if batch.Complete != nil {
		completeType = batch.Complete.Type
	}
	if batch.Success != nil {
		successType = batch.Success.Type
	}

	return &batchView{
		Bid:           bid,
		Description:   status.Description,
		CreatedAt:     status.CreatedAt,
		Total:         status.Total,
		Pending:       status.Pending,
		Failed:        status.Failed,
		Succeeded:     succeeded,
		CompleteState: status.CompleteState,
		SuccessState:  status.SuccessState,
		ParentBid:     status.ParentBid,
		ChildCount:    childCount,
		CompleteType:  completeType,
		SuccessType:   successType,
	}, nil
}

// batchesHandler serves the list page at /batches.
func (b *BatchSubsystem) batchesHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == "POST" {
		if err := r.ParseForm(); err != nil {
			util.Warnf("batch web: failed to parse form: %v", err)
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		if r.Form.Get("action") == "delete" {
			for _, bid := range r.Form["bid"] {
				b.deleteBatchTree(r.Context(), bid)
			}
		}
		webui.Redirect(w, r, "/batches", http.StatusFound)
		return
	}

	if r.Method != "GET" {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	cursor := uint64(0)
	if c := r.URL.Query().Get("cursor"); c != "" {
		parsed, err := strconv.ParseUint(c, 10, 64)
		if err == nil {
			cursor = parsed
		}
	}

	batches, nextCursor, totalCount, err := b.listBatchPage(r.Context(), cursor, batchPageSize)
	if err != nil {
		util.Warnf("batch web: failed to list batches: %v", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	data := batchListData{
		Batches:    batches,
		TotalCount: totalCount,
		NextCursor: nextCursor,
		Root:       r.Header.Get("X-Script-Name"),
		CsrfToken:  nosurf.Token(r),
	}

	webui.Layout(w, r, func() {
		err := batchListTmpl.Execute(w, data)
		if err != nil {
			util.Warnf("batch web: template error: %v", err)
		}
	})
}

// batchDetailHandler serves the detail page at /batches/{bid}.
func (b *BatchSubsystem) batchDetailHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Extract BID from URL path: /batches/{bid}
	// Take only the first path segment to ignore trailing segments like /batches/b-123/extra.
	bid := strings.TrimPrefix(r.URL.Path, "/batches/")
	if i := strings.IndexByte(bid, '/'); i >= 0 {
		bid = bid[:i]
	}
	if bid == "" {
		webui.Redirect(w, r, "/batches", http.StatusFound)
		return
	}

	batch, children, err := b.getBatchDetail(r.Context(), bid)
	if err != nil {
		util.Warnf("batch web: failed to load batch detail %s: %v", bid, err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	data := batchDetailData{
		Batch:    batch,
		Children: children,
		Root:     r.Header.Get("X-Script-Name"),
	}

	webui.Layout(w, r, func() {
		err := batchDetailTmpl.Execute(w, data)
		if err != nil {
			util.Warnf("batch web: template error: %v", err)
		}
	})
}

// callbackStateBadge returns the display label for a callback state.
func callbackStateBadge(state string) string {
	switch state {
	case CallbackEnqueued:
		return "enqueued"
	case CallbackFinished:
		return "finished"
	default:
		return "pending"
	}
}

// callbackBadgeClass returns the Bootstrap badge class for a callback state.
func callbackBadgeClass(state string) string {
	switch state {
	case CallbackEnqueued:
		return "bg-primary"
	case CallbackFinished:
		return "bg-success"
	default:
		return "bg-secondary"
	}
}

// Template data types

type batchListData struct {
	Batches    []batchView
	TotalCount int64
	NextCursor uint64
	Root       string
	CsrfToken  string
}

type batchDetailData struct {
	Batch    *batchView
	Children []batchView
	Root     string
}

// Template functions
var tmplFuncs = template.FuncMap{
	"callbackBadge":      callbackStateBadge,
	"callbackBadgeClass": callbackBadgeClass,
	"relativeTime": func(ts string) string {
		t, err := util.ParseTime(ts)
		if err != nil {
			return ts
		}
		return webui.Timeago(t)
	},
}

// batchListTmpl renders the batch list page.
var batchListTmpl = template.Must(template.New("batchList").Funcs(tmplFuncs).Parse(`
<header>
  <h3>Batches ({{.TotalCount}} total)</h3>
</header>

{{if .Batches}}
<form action="{{.Root}}/batches" method="post">
  <input type="hidden" name="csrf_token" value="{{.CsrfToken}}"/>

  <div class="table-responsive">
    <table class="table table-hover table-bordered table-striped table-light">
      <thead>
        <tr>
          <th class="checkbox-column"><input type="checkbox" class="check_all" /></th>
          <th>BID</th>
          <th>Description</th>
          <th>Created</th>
          <th>Total</th>
          <th>Pending</th>
          <th>Failed</th>
          <th>Succeeded</th>
          <th>Complete CB</th>
          <th>Success CB</th>
        </tr>
      </thead>
      <tbody>
        {{range .Batches}}
        <tr>
          <td><input type="checkbox" name="bid" value="{{.Bid}}" /></td>
          <td><a href="{{$.Root}}/batches/{{.Bid}}">{{.Bid}}</a></td>
          <td>{{.Description}}</td>
          <td>{{relativeTime .CreatedAt}}</td>
          <td>{{.Total}}</td>
          <td>{{.Pending}}</td>
          <td>{{if gt .Failed 0}}<span class="badge bg-danger">{{.Failed}}</span>{{else}}{{.Failed}}{{end}}</td>
          <td>{{.Succeeded}}</td>
          <td><span class="badge {{callbackBadgeClass .CompleteState}}">{{callbackBadge .CompleteState}}</span></td>
          <td><span class="badge {{callbackBadgeClass .SuccessState}}">{{callbackBadge .SuccessState}}</span></td>
        </tr>
        {{end}}
      </tbody>
    </table>
  </div>
  <div class="row">
    <div class="col-5">
      <button class="btn btn-danger" type="submit" name="action" value="delete" data-confirm="Are you sure?">Delete</button>
    </div>
    <div class="col-7 d-flex justify-content-end">
      {{if gt .NextCursor 0}}
      <a href="{{.Root}}/batches?cursor={{.NextCursor}}" class="btn btn-primary">Next &gt;</a>
      {{end}}
    </div>
  </div>
</form>

{{else}}
<div class="alert alert-info" role="alert">
  No active batches
</div>
{{end}}
`))

// batchDetailTmpl renders the batch detail page.
var batchDetailTmpl = template.Must(template.New("batchDetail").Funcs(tmplFuncs).Parse(`
{{with .Batch}}
<header>
  <h3>Batch {{.Bid}}</h3>
</header>

<div class="table-responsive mb-4">
  <table class="table table-bordered table-light">
    <tbody>
      <tr>
        <th style="width: 200px;">BID</th>
        <td>{{.Bid}}</td>
      </tr>
      <tr>
        <th>Description</th>
        <td>{{.Description}}</td>
      </tr>
      <tr>
        <th>Created</th>
        <td>{{relativeTime .CreatedAt}}</td>
      </tr>
      {{if .ParentBid}}
      <tr>
        <th>Parent Batch</th>
        <td><a href="{{$.Root}}/batches/{{.ParentBid}}">{{.ParentBid}}</a></td>
      </tr>
      {{end}}
    </tbody>
  </table>
</div>

<h5>Counters</h5>
<div class="table-responsive mb-4">
  <table class="table table-bordered table-light">
    <thead>
      <tr>
        <th>Total</th>
        <th>Pending</th>
        <th>Failed</th>
        <th>Succeeded</th>
      </tr>
    </thead>
    <tbody>
      <tr>
        <td>{{.Total}}</td>
        <td>{{.Pending}}</td>
        <td>{{if gt .Failed 0}}<span class="badge bg-danger">{{.Failed}}</span>{{else}}{{.Failed}}{{end}}</td>
        <td>{{.Succeeded}}</td>
      </tr>
    </tbody>
  </table>
</div>

<h5>Callbacks</h5>
<div class="table-responsive mb-4">
  <table class="table table-bordered table-light">
    <thead>
      <tr>
        <th>Callback</th>
        <th>Job Type</th>
        <th>State</th>
      </tr>
    </thead>
    <tbody>
      <tr>
        <td>Complete</td>
        <td>{{if .CompleteType}}{{.CompleteType}}{{else}}<em>not defined</em>{{end}}</td>
        <td><span class="badge {{callbackBadgeClass .CompleteState}}">{{callbackBadge .CompleteState}}</span></td>
      </tr>
      <tr>
        <td>Success</td>
        <td>{{if .SuccessType}}{{.SuccessType}}{{else}}<em>not defined</em>{{end}}</td>
        <td><span class="badge {{callbackBadgeClass .SuccessState}}">{{callbackBadge .SuccessState}}</span></td>
      </tr>
    </tbody>
  </table>
</div>
{{end}}

{{if .Children}}
<h5>Child Batches ({{len .Children}})</h5>
<div class="table-responsive">
  <table class="table table-hover table-bordered table-striped table-light">
    <thead>
      <tr>
        <th>BID</th>
        <th>Description</th>
        <th>Total</th>
        <th>Pending</th>
        <th>Failed</th>
        <th>Succeeded</th>
        <th>Complete CB</th>
        <th>Success CB</th>
      </tr>
    </thead>
    <tbody>
      {{range .Children}}
      <tr>
        <td><a href="{{$.Root}}/batches/{{.Bid}}">{{.Bid}}</a></td>
        <td>{{.Description}}</td>
        <td>{{.Total}}</td>
        <td>{{.Pending}}</td>
        <td>{{if gt .Failed 0}}<span class="badge bg-danger">{{.Failed}}</span>{{else}}{{.Failed}}{{end}}</td>
        <td>{{.Succeeded}}</td>
        <td><span class="badge {{callbackBadgeClass .CompleteState}}">{{callbackBadge .CompleteState}}</span></td>
        <td><span class="badge {{callbackBadgeClass .SuccessState}}">{{callbackBadge .SuccessState}}</span></td>
      </tr>
      {{end}}
    </tbody>
  </table>
</div>
{{end}}
`))
