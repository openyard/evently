package edge

import (
	"context"
	"log"
	"time"

	"customer/internal/app/customer/domain"
	"customer/pkg/genproto/grpcapi"

	"github.com/openyard/evently/command"
	"github.com/openyard/evently/command/async"
	"github.com/openyard/evently/pkg/uuid"
)

var _ grpcapi.CustomerServer = (*GrpcTransport)(nil)

type GrpcTransportOption func(a *GrpcTransport)

type GrpcTransport struct {
	grpcapi.UnimplementedCustomerServer
	cmdHandle command.HandleFunc
}

func NewGrpcTransport(cmdSvc command.HandleFunc, opts ...GrpcTransportOption) *GrpcTransport {
	t := &GrpcTransport{
		cmdHandle: cmdSvc,
	}
	for _, opt := range opts {
		opt(t)
	}
	return t
}

func WithAsyncWriteSide() GrpcTransportOption {
	return func(t *GrpcTransport) {
		q := async.NewCommandQueue(2, async.WithQueueTimeout(time.Millisecond*200))
		q.Register(t.cmdHandle)
		t.cmdHandle = q.Send
	}
}

func (t *GrpcTransport) OnboardCustomer(ctx context.Context, request *grpcapi.OnboardingRequest) (*grpcapi.OnboardingReply, error) {
	var sex byte
	switch request.Sex {
	case grpcapi.Sex_SEX_MALE:
		sex = 'M'
	case grpcapi.Sex_SEX_FEMALE:
		sex = 'F'
	default:
		sex = 'U'
	}
	birthday := time.Date(int(request.Birthdate.Year), time.Month(request.Birthdate.Month), int(request.Birthdate.Day),
		0, 0, 0, 0, time.Local)

	ID := uuid.NewV4().String()
	log.Printf("onboard new customer with generated ID <%s>", ID)
	err := t.cmdHandle(domain.OnboardCustomer(ID, request.Name, birthday, sex))
	if err != nil {
		return nil, err
	}
	return &grpcapi.OnboardingReply{Id: ID}, nil
}

func (t *GrpcTransport) ActivateCustomer(ctx context.Context, request *grpcapi.ActivationRequest) (*grpcapi.ActivationReply, error) {
	//TODO implement me
	panic("implement me")
}

func (t *GrpcTransport) DeactivateCustomer(ctx context.Context, request *grpcapi.DeactivationRequest) (*grpcapi.DeactivationReply, error) {
	//TODO implement me
	panic("implement me")
}
