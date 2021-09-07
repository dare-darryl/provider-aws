package vpcendpoint

import (
	"context"
	"time"

	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
	"github.com/nsf/jsondiff"
	"github.com/pkg/errors"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/ec2"
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

func postCreate(ctx context.Context, cr *svcapitypes.VPCEndpoint, obj *svcsdk.CreateVpcEndpointOutput, cre managed.ExternalCreation, err error) (managed.ExternalCreation, error) {
	if err != nil || obj.VpcEndpoint == nil {
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

	// Load DNS Entry as connection detail
	if len(resp.VpcEndpoints[0].DnsEntries) != 0 && awsclients.StringValue(resp.VpcEndpoints[0].DnsEntries[0].DnsName) != "" {
		obs.ConnectionDetails = managed.ConnectionDetails{
			xpv1.ResourceCredentialsSecretEndpointKey: []byte(awsclients.StringValue(resp.VpcEndpoints[0].DnsEntries[0].DnsName)),
		}
	}

	cr.Status.AtProvider.VPCEndpoint = generateVPCEndpointSDK(resp.VpcEndpoints[0])

	switch awsclients.StringValue(resp.VpcEndpoints[0].State) {
	case "available":
		cr.SetConditions(xpv1.Available())
	case "pending", "pending-acceptance":
		cr.SetConditions(xpv1.Creating())
	case "deleted":
		cr.SetConditions(xpv1.Unavailable())
	case "deleting":
		cr.SetConditions(xpv1.Deleting())
	}

	return obs, nil
}

/*
isUpToDate checks for the following mutable fields for the VPCEndpoint in upstream AWS:
1. Subnets
2. Security Groups
3. Route Tables
4. Policy Document
*/
func isUpToDate(cr *svcapitypes.VPCEndpoint, obj *svcsdk.DescribeVpcEndpointsOutput) (bool, error) {
	/*
		1. Check subnets
	*/
	if !listCompareStringPtrIsSame(obj.VpcEndpoints[0].SubnetIds, cr.Spec.ForProvider.SubnetIDs) {
		return false, nil
	}

	/*
		2. Check Route Tables
	*/
	if !listCompareStringPtrIsSame(obj.VpcEndpoints[0].RouteTableIds, cr.Spec.ForProvider.RouteTableIDs) {
		return false, nil
	}

	/*
		3. Check Security Groups
	*/
	upstreamSGs := obj.VpcEndpoints[0].Groups
	if len(upstreamSGs) != len(cr.Spec.ForProvider.SecurityGroupIDs) {
		return false, nil
	}

sgCompare:
	for _, declaredSG := range cr.Spec.ForProvider.SecurityGroupIDs {
		for _, upstreamSG := range upstreamSGs {
			if awsclients.StringValue(declaredSG) == awsclients.StringValue(upstreamSG.GroupId) {
				continue sgCompare
			}
		}
		// declaredSG not found in upstream AWS
		return false, nil
	}

	/*
		4. Check policyDocument
	*/
	defaultPolicy := "{\"Statement\":[{\"Action\":\"*\",\"Effect\": \"Allow\",\"Principal\":\"*\",\"Resource\":\"*\"}]}"
	declaredPolicy := awsclients.StringValue(cr.Spec.ForProvider.PolicyDocument)
	upstreamPolicy := awsclients.StringValue(obj.VpcEndpoints[0].PolicyDocument)

	// If no declared policy, we expect the result to be equivalent to the default policy
	if declaredPolicy == "" {
		difference, _ := jsondiff.Compare([]byte(upstreamPolicy), []byte(defaultPolicy), &jsondiff.Options{})
		return difference == jsondiff.FullMatch || difference == jsondiff.SupersetMatch, nil
	}

	// If there is a declared policy, we expect the upstream policy to match
	difference, _ := jsondiff.Compare([]byte(upstreamPolicy), []byte(declaredPolicy), &jsondiff.Options{})
	return difference == jsondiff.FullMatch, nil
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

func generateVPCEndpointSDK(vpcEndpoint *ec2.VpcEndpoint) *svcapitypes.VPCEndpoint_SDK {
	vpcEndpointSDK := &svcapitypes.VPCEndpoint_SDK{}

	// Mapping vpcEndpoint -> vpcEndpoint_SDK
	vpcEndpointSDK.CreationTimestamp = &v1.Time{
		Time: *vpcEndpoint.CreationTimestamp,
	}
	vpcEndpointSDK.DNSEntries = []*svcapitypes.DNSEntry{}
	for _, dnsEntry := range vpcEndpoint.DnsEntries {
		dnsEntrySDK := svcapitypes.DNSEntry{
			DNSName:      dnsEntry.DnsName,
			HostedZoneID: dnsEntry.HostedZoneId,
		}
		vpcEndpointSDK.DNSEntries = append(vpcEndpointSDK.DNSEntries, &dnsEntrySDK)
	}
	vpcEndpointSDK.State = vpcEndpoint.State

	return vpcEndpointSDK
}
<<<<<<< HEAD
=======

/*
formatModifyVpcEndpointInput takes in a ModifyVpcEndpointInput, and sets
fields containing an empty list to nil
*/
func formatModifyVpcEndpointInput(obj *svcsdk.ModifyVpcEndpointInput) {
	if len(obj.AddSecurityGroupIds) == 0 {
		obj.AddSecurityGroupIds = nil
	}
	if len(obj.RemoveSecurityGroupIds) == 0 {
		obj.RemoveSecurityGroupIds = nil
	}
	if len(obj.AddRouteTableIds) == 0 {
		obj.AddRouteTableIds = nil
	}
	if len(obj.RemoveRouteTableIds) == 0 {
		obj.RemoveRouteTableIds = nil
	}
	if len(obj.AddSubnetIds) == 0 {
		obj.AddSubnetIds = nil
	}
	if len(obj.RemoveSubnetIds) == 0 {
		obj.RemoveSubnetIds = nil
	}
	if strings.TrimSpace(aws.StringValue(obj.PolicyDocument)) == "" {
		obj.PolicyDocument = nil
	}
}

/*
listSubtractFromStringPtr takes in 2 list of string pointers
([]*string) "base", "subtract", and returns a "result" list
of string pointers where "result" = "base" - "subtract".

Comparisons of the underlying string is done

Example:
"base": ["a", "b", "g", "x"]
"subtract": ["b", "x", "y"]
"result": ["a", "g"]
*/
func listSubtractFromStringPtr(base, subtract []*string) []*string {
	result := []*string{}

compare:
	for _, baseElem := range base {
		for _, subtractElem := range subtract {
			if aws.StringValue(baseElem) == aws.StringValue(subtractElem) {
				continue compare
			}
		}
		result = append(result, baseElem)
	}

	return result
}

/*
listCompareStringPtrIsSame takes in 2 list of string pointers,
and returns a true on the following condition:
1. The length of both lists are the same
2. All values in listA can be found in listB

Warning:
This function assumes that the values in both lists are unique, that is,
all values in listA is distinct, and all values in listB is distinct.
*/
func listCompareStringPtrIsSame(listA, listB []*string) bool {
	if len(listA) != len(listB) {
		return false
	}

compare:
	for _, elemA := range listA {
		for _, elemB := range listB {
			if awsclients.StringValue(elemA) == awsclients.StringValue(elemB) {
				continue compare
			}
		}
		return false
	}

	return true
}
>>>>>>> 58d44389 (Reduce cyclomatic complexity for isUpToDate)
