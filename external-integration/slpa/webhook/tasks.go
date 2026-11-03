package webhook

import (
	"context"
	"errors"
	"fmt"

	"github.com/OpenNSW/core/taskflow/orchestrator"
	"github.com/OpenNSW/core/taskflow/store"

	"github.com/OpenNSW/nsw-srilanka/external-integration/slpa/serviceorder"
)

// ErrUnknownEvent is returned for an event this package does not model, so the
// caller can answer the CMS rather than acting on a guess.
var ErrUnknownEvent = errors.New("slpa webhook: unknown event")

// ErrOrderNotFound is returned when no consignment here holds the order an event
// is about.
var ErrOrderNotFound = errors.New("slpa webhook: no order for this event")

// ErrNotWaitingYet is returned when the consignment holds the order but nothing
// is parked on it at this instant — the previous event is still being applied.
// The CMS should retry, which is why this is distinguished from a redelivery.
var ErrNotWaitingYet = errors.New("slpa webhook: the order is not waiting on this yet")

// TaskCompleter releases a task parked on an external event. It is the only way
// this package writes to a task: a record's data belongs to the task manager,
// which persists it as part of resuming the step and lets the workflow decide
// what happens next.
//
// stepID is the parked step, read in the same lookup that found the task, so the
// completion is for the step that was parked when the event was matched; if the
// task has moved on since, core rejects it as stale.
//
// That is why an event which does not end a lifecycle still completes the step:
// the workflow's own gateway sends it back to the same wait, so what the trader
// sees is updated and the step is open again. See the SLPA task workflows.
type TaskCompleter interface {
	CompleteTaskStep(ctx context.Context, taskID, stepID string, payload map[string]any) error
}

// TaskLister lists tasks. core's task manager satisfies it. A non-nil error means the
// tasks could not be read, never that there are none.
type TaskLister interface {
	GetTasks(ctx context.Context, filter store.TaskFilter) ([]orchestrator.TaskView, error)
}

// OrderFinder finds the service orders the CMS raised. *serviceorder.Orders satisfies it.
type OrderFinder interface {
	// Find returns the order whose slug or order number is ref; the error wraps
	// serviceorder.ErrOrderNotFound when there is none.
	Find(ctx context.Context, ref string) (serviceorder.Order, error)
	// Superseded reports whether a later order was raised in the order's flow.
	Superseded(ctx context.Context, o serviceorder.Order) (bool, error)
}

// stateQueuedExternally is the state a wait parks in while SLPA owns the next
// move. Only a task in that state is waiting to be resumed.
const stateQueuedExternally = "QUEUED_EXTERNALLY"

// taskLookup finds the task an event is about: the order is recorded with its case
// and its flow's workflow when the CMS raises it, so the waiting task is looked for
// among that flow's tasks alone.
type taskLookup struct {
	orders OrderFinder
	tasks  TaskLister
}

// parked returns the task waiting on the order ref names under the given wait
// template, and the ID of the step it is parked on.
//
// The three outcomes are deliberately different, because they tell the CMS
// different things: a task to resume, a lifecycle already finished (its retries
// should stop), and an order mid-transition (its retry will land).
//
// Every answer is about this order's wait alone. A flow raises a new order when
// SLPA rejects the last one, and runs a new approval wait for it, so the waits of
// the flow's earlier orders are left out: those started before the task that
// raised this order. An event for an order the flow has since replaced is
// answered as applied, since the flow no longer waits on it.
func (l taskLookup) parked(ctx context.Context, templateID, ref string) (taskID, stepID string, err error) {
	order, err := l.orders.Find(ctx, ref)
	if errors.Is(err, serviceorder.ErrOrderNotFound) {
		return "", "", ErrOrderNotFound
	}
	if err != nil {
		return "", "", fmt.Errorf("slpa webhook: failed to look up the order: %w", err)
	}

	superseded, err := l.orders.Superseded(ctx, order)
	if err != nil {
		return "", "", fmt.Errorf("slpa webhook: failed to look up the order: %w", err)
	}
	if superseded {
		return "", "", nil
	}

	waits, err := l.waits(ctx, order, templateID)
	if err != nil {
		return "", "", err
	}

	var parked []orchestrator.TaskView
	completed := false
	for _, w := range waits {
		switch w.State {
		case stateQueuedExternally:
			parked = append(parked, w)
		case store.StateCompleted:
			completed = true
		}
	}
	switch {
	case len(parked) == 1:
		return parked[0].TaskID, parked[0].StepID, nil
	case len(parked) > 1:
		// One flow runs one wait per order at a time. Picking one would complete a
		// step at random, so the CMS is answered with a server error and retries.
		return "", "", fmt.Errorf("slpa webhook: %d tasks of workflow %s wait on order %s under %s",
			len(parked), order.WorkflowID, order.Slug, templateID)
	case completed:
		// The wait ran to the end of its flow, so this event has been applied
		// already and the CMS is answered rather than told to retry — which is
		// what stops the retries.
		return "", "", nil
	default:
		// Either the wait is between steps — the previous event is still being
		// applied and the one after it has not reopened yet — or the flow has not
		// reached it: a decision arriving before the task that waits for it.
		// Answering "already applied" would drop an event that was never acted
		// on, so the CMS is asked to retry, and its retry lands.
		return "", "", ErrNotWaitingYet
	}
}

// waits returns the tasks of order's flow that wait, or waited, on templateID for
// this order: those started since the task that raised it.
func (l taskLookup) waits(ctx context.Context, order serviceorder.Order, templateID string) ([]orchestrator.TaskView, error) {
	flow := store.TaskFilter{CaseID: order.CaseID, ParentWorkflowID: order.WorkflowID}
	tasks, err := l.tasks.GetTasks(ctx, flow)
	if err != nil {
		return nil, fmt.Errorf("slpa webhook: failed to read the tasks of workflow %s: %w", order.WorkflowID, err)
	}
	var raisedAt orchestrator.TaskView
	for _, t := range tasks {
		if t.TaskID == order.TaskID {
			raisedAt = t
			break
		}
	}

	flow.ActiveTemplateID = templateID
	candidates, err := l.tasks.GetTasks(ctx, flow)
	if err != nil {
		return nil, fmt.Errorf("slpa webhook: failed to read the tasks of workflow %s: %w", order.WorkflowID, err)
	}
	waits := candidates[:0]
	for _, t := range candidates {
		if t.TaskID != order.TaskID && !t.CreatedAt.Before(raisedAt.CreatedAt) {
			waits = append(waits, t)
		}
	}
	return waits, nil
}
