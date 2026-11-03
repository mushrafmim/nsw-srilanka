package serviceorder

import (
	"context"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func newOrders(t *testing.T) (*Orders, sqlmock.Sqlmock) {
	t.Helper()

	conn, mock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	db, err := gorm.Open(postgres.New(postgres.Config{Conn: conn}), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err)
	return NewOrders(db), mock
}

// A raised order is recorded with the task, case and flow it was raised in, and
// recording it again leaves it as it was.
func TestInterpreter_RecordsTheOrderItRaised(t *testing.T) {
	orders, mock := newOrders(t)
	mock.ExpectBegin()
	mock.ExpectExec(`INSERT INTO "slpa_service_orders" .* ON CONFLICT \("slug"\) DO NOTHING`).
		WithArgs("8d326f3a", "SO-FCL-EXPORT-2026-262316", "slpa_2_0_service_order:1", "consignment-1", "flow-1", sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	err := NewSundryInterpreter(orders).RecordDispatch(context.Background(), "slpa_2_0_service_order:1", "consignment-1", "flow-1",
		map[string]any{"slug": " 8d326f3a ", "service_order_no": "SO-FCL-EXPORT-2026-262316", "status": "client_new_actclk"})

	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

// An order without a slug could never be matched to its webhooks: the failure is
// reported (and logged by the plugin) rather than recorded as unmatchable.
func TestInterpreter_RefusesToRecordAnOrderWithoutASlug(t *testing.T) {
	orders, mock := newOrders(t)

	err := NewInterpreter(orders).RecordDispatch(context.Background(), "task-1", "consignment-1", "flow-1",
		map[string]any{"service_order_no": "SO-1"})

	require.ErrorContains(t, err, "without a slug")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestOrders_Find(t *testing.T) {
	t.Run("by slug or order number", func(t *testing.T) {
		orders, mock := newOrders(t)
		mock.ExpectQuery(`SELECT \* FROM "slpa_service_orders" WHERE slug = \$1 OR \(service_order_no <> '' AND service_order_no = \$2\) ORDER BY created_at DESC`).
			WithArgs("SO-1", "SO-1", 1).
			WillReturnRows(sqlmock.NewRows([]string{"slug", "service_order_no", "task_id", "case_id", "workflow_id"}).
				AddRow("s-1", "SO-1", "task-1", "consignment-1", "flow-1"))

		o, err := orders.Find(context.Background(), "SO-1")

		require.NoError(t, err)
		assert.Equal(t, Order{Slug: "s-1", ServiceOrderNo: "SO-1", TaskID: "task-1", CaseID: "consignment-1", WorkflowID: "flow-1"}, o)
	})

	t.Run("none recorded", func(t *testing.T) {
		orders, mock := newOrders(t)
		mock.ExpectQuery(`SELECT \* FROM "slpa_service_orders"`).WillReturnError(gorm.ErrRecordNotFound)

		_, err := orders.Find(context.Background(), "s-1")

		assert.ErrorIs(t, err, ErrOrderNotFound)
	})

	t.Run("a failed read is not a missing order", func(t *testing.T) {
		orders, mock := newOrders(t)
		mock.ExpectQuery(`SELECT \* FROM "slpa_service_orders"`).WillReturnError(assert.AnError)

		_, err := orders.Find(context.Background(), "s-1")

		require.ErrorIs(t, err, assert.AnError)
		assert.NotErrorIs(t, err, ErrOrderNotFound)
	})
}

// An order is superseded by a later one raised in the same flow of the same case.
func TestOrders_Superseded(t *testing.T) {
	raised := time.Date(2026, 8, 26, 2, 0, 0, 0, time.UTC)
	for name, later := range map[string]int{"latest": 0, "replaced": 1} {
		t.Run(name, func(t *testing.T) {
			orders, mock := newOrders(t)
			mock.ExpectQuery(`SELECT count\(\*\) FROM "slpa_service_orders" WHERE case_id = \$1 AND workflow_id = \$2 AND created_at > \$3 AND slug <> \$4`).
				WithArgs("consignment-1", "flow-1", raised, "s-1").
				WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(later))

			superseded, err := orders.Superseded(context.Background(),
				Order{Slug: "s-1", CaseID: "consignment-1", WorkflowID: "flow-1", CreatedAt: raised})

			require.NoError(t, err)
			assert.Equal(t, later > 0, superseded)
		})
	}
}
