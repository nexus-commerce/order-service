package server

import (
	"context"
	"errors"
	"fmt"
	pb "github.com/nexus-commerce/nexus-contracts-go/order/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"log"
	"order-service/internal/service"
)

type Server struct {
	pb.UnimplementedOrderServiceServer
	Service *service.Service
}

func NewOrderServer(s *service.Service) *Server {
	return &Server{
		Service: s,
	}
}

func (s *Server) CreateOrder(ctx context.Context, r *pb.CreateOrderRequest) (*pb.CreateOrderResponse, error) {
	userID, ok := ctx.Value("user-id").(int)
	if !ok {
		return nil, status.Error(codes.FailedPrecondition, "user id missing") // return FAILED_PRECONDITION status here as the system should never get into this state
	}

	userIDInt := int64(userID)

	var paymentIntentID *string
	var hasPaymentIntentID bool

	if p, ok := r.GetPaymentInfo().(*pb.CreateOrderRequest_PaymentIntentId); ok && p != nil {
		paymentIntentID = &p.PaymentIntentId
		hasPaymentIntentID = true
	} else {
		hasPaymentIntentID = false
	}

	if r.GetPaymentMethod() == pb.PaymentMethod_PAYMENT_METHOD_CARD {
		if !hasPaymentIntentID {
			return nil, status.Error(codes.FailedPrecondition, "payment_intent_id is required for card payment method")
		}
	} else if r.GetPaymentMethod() == pb.PaymentMethod_PAYMENT_METHOD_ON_DELIVERY {
		if hasPaymentIntentID {
			return nil, status.Error(codes.FailedPrecondition, "payment_intent_id must be empty for on_delivery payment method")
		}
	} else {
		return nil, status.Error(codes.InvalidArgument, "invalid payment method")
	}

	orderID, orderStatus, err := s.Service.CreateOrderSaga(ctx, userIDInt, r.GetShippingAddress(), r.GetPaymentMethod().String(), paymentIntentID)
	if err != nil {
		log.Println(err)
		switch {
		case errors.Is(err, service.ErrEmptyCart):
			return nil, status.Error(codes.FailedPrecondition, "cart is empty")
		case errors.Is(err, service.ErrGetCart):
			return nil, status.Error(codes.Internal, "failed to get cart")
		case errors.Is(err, service.ErrMissingUserID):
			return nil, status.Error(codes.FailedPrecondition, "missing user id in context")
		}
		return nil, status.Error(codes.Internal, fmt.Sprintf("failed to create order: %v", err))
	}

	return &pb.CreateOrderResponse{
		Id:     orderID,
		Status: orderStatus,
	}, nil
}

func (s *Server) GetOrder(ctx context.Context, r *pb.GetOrderRequest) (*pb.GetOrderResponse, error) {
	userID, ok := ctx.Value("user-id").(int)
	if !ok {
		return nil, status.Error(codes.FailedPrecondition, "user id missing") // return FAILED_PRECONDITION status here as the system should never get into this state
	}

	userIDInt := int64(userID)

	order, err := s.Service.GetOrder(ctx, userIDInt, r.GetId())
	if err != nil {
		log.Println(err)
		return nil, status.Error(codes.NotFound, fmt.Sprintf("failed to get order: %v", err))
	}

	items := make([]*pb.OrderItem, len(order.Items))
	for i, item := range order.Items {
		items[i] = &pb.OrderItem{
			Quantity: item.Quantity,
			Price:    item.Price,
		}
	}

	return &pb.GetOrderResponse{
		Id:              order.ID,
		UserId:          order.UserID,
		Status:          pb.Status(pb.Status_value[string(order.Status)]),
		Items:           items,
		TotalPrice:      order.TotalPrice,
		ShippingAddress: order.ShippingAddress,
	}, nil
}

func (s *Server) ListUserOrders(ctx context.Context, _ *pb.ListUserOrdersRequest) (*pb.ListUserOrdersResponse, error) {
	userID, ok := ctx.Value("user-id").(int)
	if !ok {
		return nil, status.Error(codes.FailedPrecondition, "user id missing") // return FAILED_PRECONDITION status here as the system should never get into this state
	}

	userIDInt := int64(userID)

	orders, err := s.Service.GetUserOrders(ctx, userIDInt)
	if err != nil {
		log.Println(err)
		return nil, status.Error(codes.Internal, fmt.Sprintf("failed to get user orders: %v", err))
	}

	var ordersResponse []*pb.GetOrderResponse
	for _, order := range orders {
		items := make([]*pb.OrderItem, len(order.Items))
		for i, item := range order.Items {
			items[i] = &pb.OrderItem{
				Sku:      item.Sku,
				Quantity: item.Quantity,
				Price:    item.Price,
			}
		}
		ordersResponse = append(ordersResponse, &pb.GetOrderResponse{
			Id:              order.ID,
			UserId:          order.UserID,
			Status:          pb.Status(pb.Status_value[string(order.Status)]),
			Items:           items,
			TotalPrice:      order.TotalPrice,
			ShippingAddress: order.ShippingAddress,
		})
	}

	return &pb.ListUserOrdersResponse{
		Orders: ordersResponse,
	}, nil
}
