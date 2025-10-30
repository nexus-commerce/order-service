package repository

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"order-service/internal/model"
	"strings"
)

type Repository struct {
	conn *sql.DB
}

func New(conn *sql.DB) *Repository {
	return &Repository{conn: conn}
}

func (r *Repository) CreateOrder(ctx context.Context, order *model.Order) (int64, error) {
	tx, err := r.conn.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	orderQuery := `INSERT INTO orders (user_id, status, total_price, shipping_address, created_at)
     VALUES ($1, $2, $3, $4, $5) RETURNING id`

	err = tx.QueryRowContext(ctx, orderQuery,
		order.UserID,
		order.Status,
		order.TotalPrice,
		order.ShippingAddress,
		order.CreatedAt,
	).Scan(&order.ID)
	if err != nil {
		return 0, err
	}

	if len(order.Items) > 0 {
		valueStrings := make([]string, 0, len(order.Items))
		valueArgs := make([]interface{}, 0, len(order.Items)*4)
		i := 1
		for _, item := range order.Items {
			valueStrings = append(valueStrings,
				fmt.Sprintf("($%d, $%d, $%d, $%d)", i, i+1, i+2, i+3))
			valueArgs = append(valueArgs, order.ID, item.Quantity, item.Price, item.Sku)
			i += 4
		}

		itemsQuery := fmt.Sprintf("INSERT INTO order_items (order_id, quantity, price, sku) VALUES %s",
			strings.Join(valueStrings, ","),
		)

		_, err = tx.ExecContext(ctx, itemsQuery, valueArgs...)
		if err != nil {
			return 0, err
		}
	}

	if err := tx.Commit(); err != nil {
		return 0, err
	}

	return order.ID, nil
}

func (r *Repository) GetOrderByID(ctx context.Context, orderID int64) (*model.Order, error) {
	query := `SELECT o.id, o.user_id, o.status, o.total_price, o.shipping_address, o.created_at,
       oi.id, oi.quantity, oi.price, oi.sku
 FROM orders o
 LEFT JOIN order_items oi ON o.id = oi.order_id
 WHERE o.id = $1`

	rows, err := r.conn.QueryContext(ctx, query, orderID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var order *model.Order
	for rows.Next() {
		var itemID sql.NullInt64
		var itemQuantity sql.NullInt64
		var itemPrice sql.NullFloat64
		var itemSku sql.NullString

		if order == nil {
			order = &model.Order{}
			err = rows.Scan(
				&order.ID, &order.UserID, &order.Status, &order.TotalPrice, &order.ShippingAddress, &order.CreatedAt,
				&itemID, &itemQuantity, &itemPrice, &itemSku,
			)
		} else {
			var tempOrderID, tempUserID int64
			var tempStatus model.Status
			var tempTotalPrice float64
			var tempShippingAddress string
			var tempCreatedAt sql.NullTime
			err = rows.Scan(
				&tempOrderID, &tempUserID, &tempStatus, &tempTotalPrice, &tempShippingAddress, &tempCreatedAt,
				&itemID, &itemQuantity, &itemPrice, &itemSku,
			)
		}

		if err != nil {
			return nil, err
		}

		if itemID.Valid {
			item := &model.OrderItem{
				ID:       itemID.Int64,
				OrderID:  orderID,
				Quantity: itemQuantity.Int64,
				Price:    itemPrice.Float64,
				Sku:      itemSku.String,
			}
			order.Items = append(order.Items, item)
		}
	}

	if err = rows.Err(); err != nil {
		return nil, err
	}

	if order == nil {
		return nil, errors.New("order not found")
	}

	return order, nil
}

func (r *Repository) GetUserOrders(ctx context.Context, userID int64) ([]*model.Order, error) {
	query := `SELECT o.id, o.user_id, o.status, o.total_price, o.shipping_address, o.created_at,
       oi.id, oi.quantity, oi.price, oi.sku
	FROM orders o
	LEFT JOIN order_items oi ON o.id = oi.order_id 
	WHERE o.user_id = $1`

	rows, err := r.conn.QueryContext(ctx, query, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	ordersMap := make(map[int64]*model.Order)
	for rows.Next() {
		var orderID int64
		var item model.OrderItem
		var itemID sql.NullInt64
		var itemQuantity sql.NullInt64
		var itemPrice sql.NullFloat64
		var itemSku sql.NullString

		var tempOrder model.Order

		err = rows.Scan(
			&orderID, &tempOrder.UserID, &tempOrder.Status, &tempOrder.TotalPrice, &tempOrder.ShippingAddress, &tempOrder.CreatedAt,
			&itemID, &itemQuantity, &itemPrice, &itemSku,
		)
		if err != nil {
			return nil, err
		}

		order, exists := ordersMap[orderID]
		if !exists {
			order = &model.Order{
				ID:              orderID,
				UserID:          tempOrder.UserID,
				Status:          tempOrder.Status,
				TotalPrice:      tempOrder.TotalPrice,
				ShippingAddress: tempOrder.ShippingAddress,
				CreatedAt:       tempOrder.CreatedAt,
				Items:           []*model.OrderItem{},
			}
			ordersMap[orderID] = order
		}

		if itemID.Valid {
			item.ID = itemID.Int64
			item.OrderID = orderID
			item.Quantity = itemQuantity.Int64
			item.Price = itemPrice.Float64
			item.Sku = itemSku.String
			order.Items = append(order.Items, &item)
		}
	}

	if err = rows.Err(); err != nil {
		return nil, err
	}

	orders := make([]*model.Order, 0, len(ordersMap))
	for _, order := range ordersMap {
		orders = append(orders, order)
	}

	return orders, nil
}

func (r *Repository) UpdateOrderStatus(ctx context.Context, orderID int64, status model.Status) error {
	query := `UPDATE orders SET status = $1 WHERE id = $2`
	_, err := r.conn.ExecContext(ctx, query, status, orderID)
	return err
}
