package webhook

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// completer records what the service asked the task manager to do.
type orderCompleter struct {
	taskID  string
	stepID  string
	payload map[string]any
	called  bool
}

func (c *orderCompleter) CompleteTaskStep(_ context.Context, taskID, stepID string, payload map[string]any) error {
	c.taskID, c.stepID, c.payload, c.called = taskID, stepID, payload, true
	return nil
}

func newOrderEvents(flow *fakeFlow) (*OrderEvents, *orderCompleter) {
	tasks := &orderCompleter{}
	return NewOrderEvents(flow, flow, tasks), tasks
}

// parkedOn returns the flow with its approval wait taskID parked on the order.
func parkedOn(taskID string) *fakeFlow {
	return newFlow().wait(ApprovalWaitTemplateID, taskID, stateQueuedExternally, 1)
}

func orderEvent(name string) OrderEvent {
	return OrderEvent{
		Event:          name,
		Slug:           slug,
		ServiceOrderNo: "SO-FCL-EXPORT-2026-262316",
		CusdecSerial:   "BIBE1CBEX1-2026-E-10642026",
		Status:         "act_approve",
		InvoiceNo:      "INV-2026-04412",
		TotalAmount:    16,
		Timestamp:      "2026-08-26T02:30:59.846Z",
	}
}

// The accountant's approval is the end of SLPA's review, so the step waiting on
// it is released and the flow moves on.
func TestOrderEvents_ApprovalReleasesTheStep(t *testing.T) {
	flow := parkedOn("slpa_3_0_decision:abc")
	service, tasks := newOrderEvents(flow)

	require.NoError(t, service.Handle(context.Background(), orderEvent(EventApprovedByAccountant)))
	assert.Equal(t, "consignment-1", flow.filters[0].CaseID, "the wait is looked for in the order's case")
	assert.Equal(t, "flow-1", flow.filters[0].ParentWorkflowID, "and in the flow that raised it")

	require.True(t, tasks.called)
	assert.Equal(t, "slpa_3_0_decision:abc", tasks.taskID)
	assert.Equal(t, "step-slpa_3_0_decision:abc", tasks.stepID)
	assert.Equal(t, "submit", tasks.payload["__command"])
	assert.Equal(t, DecisionApproved, tasks.payload["decision"])
	assert.Equal(t, true, tasks.payload["final"])
	assert.Equal(t, "Approved by the accountant", tasks.payload["stage"])
	assert.Equal(t, "INV-2026-04412", tasks.payload["invoice_no"])
	assert.Equal(t, float64(16), tasks.payload["total_amount"])
}

// Either refusal ends SLPA's review too: the trader has to act, so the step must
// not sit waiting for an approval that is not coming.
func TestOrderEvents_RejectionReleasesTheStep(t *testing.T) {
	for _, name := range []string{EventRejectedByAccountsClerk, EventRejectedByAccountant} {
		t.Run(name, func(t *testing.T) {
			service, tasks := newOrderEvents(parkedOn("task-1"))

			require.NoError(t, service.Handle(context.Background(), orderEvent(name)))
			assert.Equal(t, DecisionRejected, tasks.payload["decision"])
		})
	}
}

// The clerk's approval, and a return to the clerk, leave the order with SLPA.
// The trader is shown where it is; the step stays open.
// The clerk's approval, and a return to the clerk, leave the order with SLPA.
// They still go through the task manager — nothing here edits a record in place —
// and final:false is what sends the workflow back to the same wait.
func TestOrderEvents_ProgressGoesThroughTheTaskManagerToo(t *testing.T) {
	for name, stage := range map[string]string{
		EventApprovedByAccountsClerk: "Approved by the accounts clerk, with the accountant",
		EventRejectedToAccountsClerk: "Returned by the accountant to the accounts clerk",
	} {
		t.Run(name, func(t *testing.T) {
			service, tasks := newOrderEvents(parkedOn("task-1"))

			require.NoError(t, service.Handle(context.Background(), orderEvent(name)))

			require.True(t, tasks.called)
			assert.Equal(t, false, tasks.payload["final"], "the order is still with SLPA")
			assert.Equal(t, DecisionPending, tasks.payload["decision"])
			assert.Equal(t, stage, tasks.payload["stage"])
		})
	}
}

// A redelivery of a decision already applied finds nothing parked. The CMS is
// answered rather than told to retry, which is what stops the retries.
func TestOrderEvents_RedeliveryIsAnswered(t *testing.T) {
	service, tasks := newOrderEvents(newFlow().wait(ApprovalWaitTemplateID, "task-1", "COMPLETED", 1))

	require.NoError(t, service.Handle(context.Background(), orderEvent(EventApprovedByAccountant)))
	assert.False(t, tasks.called)
}

// An order this deployment never raised is a different matter: the CMS is told
// so, and may retry in case the two are racing.
func TestOrderEvents_UnknownOrder(t *testing.T) {
	service, tasks := newOrderEvents(&fakeFlow{})

	assert.ErrorIs(t, service.Handle(context.Background(), orderEvent(EventApprovedByAccountant)), ErrOrderNotFound)
	assert.False(t, tasks.called)
}

// The decision arrives before the task that waits for it — the consignment holds
// the order, but this wait has not opened yet. Their retry resolves it.
func TestOrderEvents_BeforeTheWaitOpens(t *testing.T) {
	service, tasks := newOrderEvents(newFlow())

	assert.ErrorIs(t, service.Handle(context.Background(), orderEvent(EventApprovedByAccountant)), ErrNotWaitingYet)
	assert.False(t, tasks.called)
}

// A redelivered final approval must be answered even while the invoice task that
// followed it is open: judging the redelivery by every task of the flow would let
// that sibling say "retry" for a decision already applied — and SLPA would keep
// retrying it for as long as the invoice went unpaid.
func TestOrderEvents_RedeliveryIsAnsweredWhileTheInvoiceTaskRuns(t *testing.T) {
	service, tasks := newOrderEvents(newFlow().
		wait(ApprovalWaitTemplateID, "approval", "COMPLETED", 1).
		wait(PaymentWaitTemplateID, "invoice", stateQueuedExternally, 2))

	require.NoError(t, service.Handle(context.Background(), orderEvent(EventApprovedByAccountant)))
	assert.False(t, tasks.called)
}

// Between steps the wait is neither parked nor finished: the previous event is
// still being applied. Answering "already applied" would drop an event nothing
// acted on, so the CMS is asked to retry.
func TestOrderEvents_BetweenStepsAsksForARetry(t *testing.T) {
	service, tasks := newOrderEvents(newFlow().wait(ApprovalWaitTemplateID, "task-1", "STARTING_STEP", 1))

	assert.ErrorIs(t, service.Handle(context.Background(), orderEvent(EventApprovedByAccountant)), ErrNotWaitingYet)
	assert.False(t, tasks.called)
}

// SLPA rejected the flow's first order and the trader raised another. The first
// order's approval wait finished, but it does not answer for the new order: a
// decision on the new order before its wait opens is retried, not dropped.
func TestOrderEvents_AnEarlierOrdersWaitDoesNotAnswerForANewOrder(t *testing.T) {
	flow := &fakeFlow{}
	flow.raise("first-order", "SO-1", workflowID, "slpa_2_0_service_order:1", 0)
	flow.wait(ApprovalWaitTemplateID, "slpa_3_0_approval:1", "COMPLETED", 1)
	flow.raise(slug, serviceOrderNo, workflowID, "slpa_2_0_service_order:2", 10)
	service, tasks := newOrderEvents(flow)

	assert.ErrorIs(t, service.Handle(context.Background(), orderEvent(EventApprovedByAccountant)), ErrNotWaitingYet)
	assert.False(t, tasks.called)

	flow.wait(ApprovalWaitTemplateID, "slpa_3_0_approval:2", stateQueuedExternally, 11)
	require.NoError(t, service.Handle(context.Background(), orderEvent(EventApprovedByAccountant)))
	assert.Equal(t, "slpa_3_0_approval:2", tasks.taskID)
}

// A late event about an order the flow has since replaced is answered, so the
// CMS stops retrying it, and never reaches the new order's wait.
func TestOrderEvents_AReplacedOrderIsAnswered(t *testing.T) {
	flow := &fakeFlow{}
	flow.raise(slug, serviceOrderNo, workflowID, "slpa_2_0_service_order:1", 0)
	flow.wait(ApprovalWaitTemplateID, "slpa_3_0_approval:1", "COMPLETED", 1)
	flow.raise("second-order", "SO-2", workflowID, "slpa_2_0_service_order:2", 10)
	flow.wait(ApprovalWaitTemplateID, "slpa_3_0_approval:2", stateQueuedExternally, 11)
	service, tasks := newOrderEvents(flow)

	require.NoError(t, service.Handle(context.Background(), orderEvent(EventRejectedByAccountant)))
	assert.False(t, tasks.called)
}

// Each flow on the consignment waits on its own order: a sundry order's wait is
// not the one a decision on the main order releases.
func TestOrderEvents_AnotherFlowsWaitIsNotReleased(t *testing.T) {
	service, tasks := newOrderEvents(newFlow().
		waitIn("sundry-flow", ApprovalWaitTemplateID, "sundry-approval", stateQueuedExternally, 1))

	assert.ErrorIs(t, service.Handle(context.Background(), orderEvent(EventApprovedByAccountant)), ErrNotWaitingYet)
	assert.False(t, tasks.called)
}

// Tasks that could not be read are not "nothing is waiting": the CMS is answered
// with a server error, and its retry lands once they can be.
func TestOrderEvents_FailedReadIsNotAnAnswer(t *testing.T) {
	flow := parkedOn("task-1")
	flow.listErr = errors.New("db down")
	service, tasks := newOrderEvents(flow)

	err := service.Handle(context.Background(), orderEvent(EventApprovedByAccountant))
	require.ErrorContains(t, err, "db down")
	assert.NotErrorIs(t, err, ErrNotWaitingYet)
	assert.NotErrorIs(t, err, ErrOrderNotFound)
	assert.False(t, tasks.called)
}

// Two tasks of one flow parked on the same order is a fault, not a choice to make.
func TestOrderEvents_RefusesToPickAmongTwoParkedWaits(t *testing.T) {
	service, tasks := newOrderEvents(parkedOn("task-1").wait(ApprovalWaitTemplateID, "task-2", stateQueuedExternally, 2))

	require.Error(t, service.Handle(context.Background(), orderEvent(EventApprovedByAccountant)))
	assert.False(t, tasks.called)
}

func TestOrderEvents_RefusesWhatItCannotActOn(t *testing.T) {
	service, _ := newOrderEvents(newFlow())

	t.Run("an event that is not modelled", func(t *testing.T) {
		e := orderEvent("service_order.something_else")
		assert.ErrorIs(t, service.Handle(context.Background(), e), ErrUnknownEvent)
	})

	t.Run("no slug to correlate by", func(t *testing.T) {
		e := orderEvent(EventApprovedByAccountant)
		e.Slug = "  "
		err := service.Handle(context.Background(), e)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "needs a slug")
	})
}

func TestOrderEventDecisions(t *testing.T) {
	for name, want := range map[string]string{
		EventApprovedByAccountant:    DecisionApproved,
		EventRejectedByAccountsClerk: DecisionRejected,
		EventRejectedByAccountant:    DecisionRejected,
		EventApprovedByAccountsClerk: DecisionPending,
		EventRejectedToAccountsClerk: DecisionPending,
		"service_order.invented":     "",
	} {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, want, OrderEvent{Event: name}.Decision())
		})
	}
}
