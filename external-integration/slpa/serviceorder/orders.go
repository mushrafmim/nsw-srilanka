package serviceorder

import (
	"context"
	"errors"
	"fmt"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/OpenNSW/nsw-srilanka/external-integration/slpa/cms"
)

// ErrOrderNotFound is returned by Orders.Find when no order was recorded under the reference.
var ErrOrderNotFound = errors.New("slpa service order: no order recorded under this reference")

// Order is a service order the CMS raised, recorded when it was raised so that the webhook
// reporting its approvals and payment finds the flow that waits on it.
//
// The CMS names an order by its slug or, on some invoice events, by its order number. The
// task that raised it, the consignment's case and the flow's workflow are what the webhook
// scopes the waiting task by: the approval and payment waits run as tasks of the same flow,
// started after the task that raised the order.
type Order struct {
	Slug           string    `gorm:"column:slug;primaryKey"`
	ServiceOrderNo string    `gorm:"column:service_order_no"`
	TaskID         string    `gorm:"column:task_id"`
	CaseID         string    `gorm:"column:case_id"`
	WorkflowID     string    `gorm:"column:workflow_id"`
	CreatedAt      time.Time `gorm:"column:created_at;autoCreateTime"`
}

// TableName names the table migration 000038 creates.
func (Order) TableName() string { return "slpa_service_orders" }

// Orders records the service orders the CMS raised and finds them again.
type Orders struct {
	db *gorm.DB
}

// NewOrders returns the orders recorded in db.
func NewOrders(db *gorm.DB) *Orders { return &Orders{db: db} }

// Record records o. Recording an order already recorded leaves it as it is, so a retried
// write is harmless.
func (r *Orders) Record(ctx context.Context, o Order) error {
	return r.db.WithContext(ctx).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "slug"}},
		DoNothing: true,
	}).Create(&o).Error
}

// Find returns the order whose slug or order number is ref, or ErrOrderNotFound.
func (r *Orders) Find(ctx context.Context, ref string) (Order, error) {
	var o Order
	err := r.db.WithContext(ctx).
		Where("slug = ? OR (service_order_no <> '' AND service_order_no = ?)", ref, ref).
		Order("created_at DESC").
		First(&o).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return Order{}, fmt.Errorf("%w: %s", ErrOrderNotFound, ref)
	}
	if err != nil {
		return Order{}, fmt.Errorf("slpa service order: find the order %s: %w", ref, err)
	}
	return o, nil
}

// Superseded reports whether a later order was raised in o's flow: the flow raises a new
// order when SLPA rejects the last one, and an activity retried after the CMS answered raises
// one too. The flow goes on with the latest order alone.
func (r *Orders) Superseded(ctx context.Context, o Order) (bool, error) {
	var later int64
	err := r.db.WithContext(ctx).Model(&Order{}).
		Where("case_id = ? AND workflow_id = ? AND created_at > ? AND slug <> ?", o.CaseID, o.WorkflowID, o.CreatedAt, o.Slug).
		Count(&later).Error
	if err != nil {
		return false, fmt.Errorf("slpa service order: find the orders raised after %s: %w", o.Slug, err)
	}
	return later > 0, nil
}

// recordOrder records the order an accepted create-order call raised, from the fields
// interpret captured. It is the RecordDispatch of both order interpreters.
func recordOrder(ctx context.Context, orders *Orders, taskID, caseID, workflowID string, captured map[string]any) error {
	if orders == nil {
		return nil
	}
	slug := cms.String(captured, "slug")
	if slug == "" {
		return fmt.Errorf("slpa service order: the CMS raised an order without a slug; its webhooks cannot be matched")
	}
	return orders.Record(ctx, Order{
		Slug:           slug,
		ServiceOrderNo: cms.String(captured, "service_order_no"),
		TaskID:         taskID,
		CaseID:         caseID,
		WorkflowID:     workflowID,
	})
}
