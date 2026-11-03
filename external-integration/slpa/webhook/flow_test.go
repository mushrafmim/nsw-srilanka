package webhook

import (
	"context"
	"fmt"
	"time"

	"github.com/OpenNSW/core/taskflow/orchestrator"
	"github.com/OpenNSW/core/taskflow/store"

	"github.com/OpenNSW/nsw-srilanka/external-integration/slpa/serviceorder"
)

const (
	slug           = "8d326f3a-643a-4a1d-8072-87130288b032"
	serviceOrderNo = "SO-FCL-EXPORT-2026-262342"
	caseID         = "consignment-1"
	workflowID     = "flow-1"
	raisingTaskID  = "slpa_2_0_service_order:1"
)

var flowStart = time.Date(2026, 8, 26, 2, 0, 0, 0, time.UTC)

// flowTask is a task of a case as the fake lists it.
type flowTask struct {
	view       orchestrator.TaskView
	caseID     string
	workflowID string
	templateID string
}

// fakeFlow holds the orders recorded and the tasks of the case, and answers as
// serviceorder.Orders and core's task manager do: a listing matches every set
// filter field.
type fakeFlow struct {
	orders  []serviceorder.Order
	tasks   []flowTask
	listErr error
	filters []store.TaskFilter
}

// newFlow returns a case whose flow workflowID raised the order slug, by the task
// raisingTaskID, at flowStart.
func newFlow() *fakeFlow {
	f := &fakeFlow{}
	f.raise(slug, serviceOrderNo, workflowID, raisingTaskID, 0)
	return f
}

// raise records an order raised by taskID in workflow, minutes after flowStart.
func (f *fakeFlow) raise(slug, orderNo, workflow, taskID string, minutes int) {
	at := flowStart.Add(time.Duration(minutes) * time.Minute)
	f.orders = append(f.orders, serviceorder.Order{
		Slug: slug, ServiceOrderNo: orderNo, TaskID: taskID, CaseID: caseID, WorkflowID: workflow, CreatedAt: at,
	})
	f.tasks = append(f.tasks, flowTask{
		view:   orchestrator.TaskView{TaskID: taskID, State: store.StateCompleted, CreatedAt: at},
		caseID: caseID, workflowID: workflow, templateID: "slpa-so--create",
	})
}

// wait adds a task of workflowID on templateID, in state, started minutes after flowStart.
func (f *fakeFlow) wait(templateID, taskID, state string, minutes int) *fakeFlow {
	return f.waitIn(workflowID, templateID, taskID, state, minutes)
}

func (f *fakeFlow) waitIn(workflow, templateID, taskID, state string, minutes int) *fakeFlow {
	f.tasks = append(f.tasks, flowTask{
		view: orchestrator.TaskView{
			TaskID: taskID, State: state, StepID: "step-" + taskID,
			CreatedAt: flowStart.Add(time.Duration(minutes) * time.Minute),
		},
		caseID: caseID, workflowID: workflow, templateID: templateID,
	})
	return f
}

func (f *fakeFlow) Find(_ context.Context, ref string) (serviceorder.Order, error) {
	for _, o := range f.orders {
		if o.Slug == ref || (o.ServiceOrderNo != "" && o.ServiceOrderNo == ref) {
			return o, nil
		}
	}
	return serviceorder.Order{}, fmt.Errorf("%w: %s", serviceorder.ErrOrderNotFound, ref)
}

func (f *fakeFlow) Superseded(_ context.Context, o serviceorder.Order) (bool, error) {
	for _, other := range f.orders {
		if other.Slug != o.Slug && other.CaseID == o.CaseID && other.WorkflowID == o.WorkflowID && other.CreatedAt.After(o.CreatedAt) {
			return true, nil
		}
	}
	return false, nil
}

func (f *fakeFlow) GetTasks(_ context.Context, filter store.TaskFilter) ([]orchestrator.TaskView, error) {
	f.filters = append(f.filters, filter)
	if f.listErr != nil {
		return nil, f.listErr
	}
	var out []orchestrator.TaskView
	for _, t := range f.tasks {
		if (filter.CaseID == "" || t.caseID == filter.CaseID) &&
			(filter.ParentWorkflowID == "" || t.workflowID == filter.ParentWorkflowID) &&
			(filter.ActiveTemplateID == "" || t.templateID == filter.ActiveTemplateID) &&
			(filter.State == "" || t.view.State == filter.State) {
			out = append(out, t.view)
		}
	}
	return out, nil
}
