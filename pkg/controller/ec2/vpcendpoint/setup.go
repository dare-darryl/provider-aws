package vpcendpoint

import (
	"context"
	"time"

	"k8s.io/client-go/util/workqueue"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"

	"github.com/crossplane/crossplane-runtime/pkg/event"
	"github.com/crossplane/crossplane-runtime/pkg/logging"
	"github.com/crossplane/crossplane-runtime/pkg/meta"
	"github.com/crossplane/crossplane-runtime/pkg/ratelimiter"
	"github.com/crossplane/crossplane-runtime/pkg/reconciler/managed"
	"github.com/crossplane/crossplane-runtime/pkg/resource"
	"github.com/pkg/errors"

	"github.com/aws/aws-sdk-go/aws"
	svcsdk "github.com/aws/aws-sdk-go/service/ec2"
	svcsdkapi "github.com/aws/aws-sdk-go/service/ec2/ec2iface"
	xpv1 "github.com/crossplane/crossplane-runtime/apis/common/v1"
	cpresource "github.com/crossplane/crossplane-runtime/pkg/resource"
	svcapitypes "github.com/crossplane/provider-aws/apis/ec2/v1alpha1"
	awsclients "github.com/crossplane/provider-aws/pkg/clients"
)

// SetupVPCEndpoint adds a controller that reconciles VPCEndpoint.
func SetupVPCEndpoint(mgr ctrl.Manager, l logging.Logger, rl workqueue.RateLimiter, poll time.Duration) error {
	name := managed.ControllerName(svcapitypes.VPCEndpointGroupKind)
	opts := []option{
		func(e *external) {
			c := &custom{client: e.client, kube: e.kube}
			e.delete = c.delete
			e.preCreate = preCreate
			e.postCreate = postCreate
			e.postObserve = postObserve
			e.isUpToDate = isUpToDate
			e.filterList = filterList
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

type custom struct {
	kube   client.Client
	client svcsdkapi.EC2API
}

func preCreate(_ context.Context, cr *svcapitypes.VPCEndpoint, obj *svcsdk.CreateVpcEndpointInput) error {
	// set external name as tag on the vpc endpoint
	resType := "vpc-endpoint"
	key := "Name"
	value := meta.GetExternalName(cr)

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

func postCreate(ctx context.Context, cr *svcapitypes.VPCEndpoint, obj *svcsdk.CreateVpcEndpointOutput, cre managed.ExternalCreation, err error) (managed.ExternalCreation, error) {
	if err != nil {
		return managed.ExternalCreation{}, err
	}

	// set vpc endpoint id as external name annotation on k8s object after creation
	meta.SetExternalName(cr, aws.StringValue(obj.VpcEndpoint.VpcEndpointId))
	cre.ExternalNameAssigned = true
	return cre, nil
}

func postObserve(_ context.Context, cr *svcapitypes.VPCEndpoint, resp *svcsdk.DescribeVpcEndpointsOutput, obs managed.ExternalObservation, err error) (managed.ExternalObservation, error) {
	if err != nil {
		return managed.ExternalObservation{}, err
	}

	switch awsclients.StringValue(resp.VpcEndpoints[0].State) {
	case "available":
		cr.SetConditions(xpv1.Available())
	case "pending":
		cr.SetConditions(xpv1.Creating())
	case "pending-acceptance":
		cr.SetConditions(xpv1.Creating())
	case "deleted":
		cr.SetConditions(xpv1.Unavailable())
	case "deleting":
		cr.SetConditions(xpv1.Deleting())
	}

	return obs, nil
}

func isUpToDate(*svcapitypes.VPCEndpoint, *svcsdk.DescribeVpcEndpointsOutput) (bool, error) {
	return true, nil
}

func (e *custom) delete(_ context.Context, mg cpresource.Managed) error {
	cr, ok := mg.(*svcapitypes.VPCEndpoint)
	if !ok {
		return errors.New(errUnexpectedObject)
	}

	// Generate Deletion Input
	deleteInput := &svcsdk.DeleteVpcEndpointsInput{}
	externalName := meta.GetExternalName(cr)
	deleteInput.SetVpcEndpointIds([]*string{&externalName})

	// Delete
	_, err := e.client.DeleteVpcEndpoints(deleteInput)
	return err
}

func filterList(cr *svcapitypes.VPCEndpoint, obj *svcsdk.DescribeVpcEndpointsOutput) *svcsdk.DescribeVpcEndpointsOutput {
	connectionIdentifier := aws.String(meta.GetExternalName(cr))
	resp := &svcsdk.DescribeVpcEndpointsOutput{}
	for _, vpcEndpoint := range obj.VpcEndpoints {
		if aws.StringValue(vpcEndpoint.VpcEndpointId) == aws.StringValue(connectionIdentifier) {
			resp.VpcEndpoints = append(resp.VpcEndpoints, vpcEndpoint)
			break
		}
	}
	return resp
}
