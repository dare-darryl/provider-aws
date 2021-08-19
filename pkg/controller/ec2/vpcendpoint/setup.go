package vpcendpoint

import (
	"context"
	"time"

	"k8s.io/client-go/util/workqueue"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/controller"

	"github.com/crossplane/crossplane-runtime/pkg/event"
	"github.com/crossplane/crossplane-runtime/pkg/logging"
	"github.com/crossplane/crossplane-runtime/pkg/ratelimiter"
	"github.com/crossplane/crossplane-runtime/pkg/reconciler/managed"
	"github.com/crossplane/crossplane-runtime/pkg/resource"

	svcsdk "github.com/aws/aws-sdk-go/service/ec2"
	svcapitypes "github.com/crossplane/provider-aws/apis/ec2/v1alpha1"
)

// SetupVPCEndpoint adds a controller that reconciles VPCEndpoint.
func SetupVPCEndpoint(mgr ctrl.Manager, l logging.Logger, rl workqueue.RateLimiter, poll time.Duration) error {
	name := managed.ControllerName(svcapitypes.VPCEndpointGroupKind)
	opts := []option{
		func(e *external) {
			e.preCreate = preCreate
		},
	}
	return ctrl.NewControllerManagedBy(mgr).
		Named(name).
		WithOptions(controller.Options{
			RateLimiter: ratelimiter.NewDefaultManagedRateLimiter(rl),
		}).
		For(&svcapitypes.VPCEndpoint{}).
		Complete(managed.NewReconciler(mgr,
			resource.ManagedKind(svcapitypes.VPCEndpointGroupVersionKind),
			managed.WithExternalConnecter(&connector{kube: mgr.GetClient(), opts: opts}),
			managed.WithPollInterval(poll),
			managed.WithLogger(l.WithValues("controller", name)),
			managed.WithRecorder(event.NewAPIRecorder(mgr.GetEventRecorderFor(name)))))
}

func preCreate(_ context.Context, cr *svcapitypes.VPCEndpoint, obj *svcsdk.CreateVpcEndpointInput) error {
	// set external name as tag on the vpc endpoint
	resType := "vpc-endpoint"
	key := "Name"
	value := cr.ObjectMeta.Name

	// Clear SGs, RTs, and Subnets if they're empty
	if cr.Spec.ForProvider.SecurityGroupIDs == nil || len(cr.Spec.ForProvider.SecurityGroupIDs) == 0 {
		obj.SecurityGroupIds = nil
	}
	if cr.Spec.ForProvider.RouteTableIDs == nil || len(cr.Spec.ForProvider.RouteTableIDs) == 0 {
		obj.RouteTableIds = nil
	}
	if cr.Spec.ForProvider.SubnetIDs == nil || len(cr.Spec.ForProvider.SubnetIDs) == 0 {
		obj.SubnetIds = nil
	}

	// Set tags
	spec := svcsdk.TagSpecification{
		ResourceType: &resType,
		Tags: []*svcsdk.Tag{
			{
				Key:   &key,
				Value: &value,
			},
		},
	}
	obj.TagSpecifications = append(obj.TagSpecifications, &spec)

	return nil
}
