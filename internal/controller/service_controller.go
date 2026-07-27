package controller

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/utils/ptr"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

const (
	AnnoHostedCluster = "reflector.bizcochillo.io/hosted-cluster"
	AnnoTargetService = "reflector.bizcochillo.io/target-service"
	AnnoMode          = "reflector.bizcochillo.io/mode"
)

// ServiceReconciler reconciles a Service object in the Hub
type ServiceReconciler struct {
	client.Client
	Scheme *runtime.Scheme

	// Connection Manager to avoid memory leaks and multiple watchers
	mu          sync.Mutex
	Connections map[string]context.CancelFunc

	// Channel to receive dynamic events from the Hosted Clusters
	TriggerChannel chan event.GenericEvent

	RemoteClientBuilder func(kubeconfigBytes []byte) (kubernetes.Interface, error)
}

// +kubebuilder:rbac:groups=core,resources=services,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=core,resources=secrets,verbs=get;list;watch
// +kubebuilder:rbac:groups=discovery.k8s.io,resources=endpointslices,verbs=get;list;watch;create;update;patch;delete

func (r *ServiceReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	// 1. Fetch the Hub Headless Service
	hubSvc := &corev1.Service{}
	err := r.Get(ctx, req.NamespacedName, hubSvc)
	if err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// 2. Extract configuration from Annotations
	hcAnno := hubSvc.Annotations[AnnoHostedCluster]
	targetSvcAnno := hubSvc.Annotations[AnnoTargetService]

	hcParts := strings.Split(hcAnno, "/")
	if len(hcParts) != 2 {
		return ctrl.Result{}, nil
	}
	hcNamespace, hcName := hcParts[0], hcParts[1]

	targetSvcParts := strings.Split(targetSvcAnno, "/")
	if len(targetSvcParts) != 2 {
		return ctrl.Result{}, nil
	}
	targetNs, targetName := targetSvcParts[0], targetSvcParts[1]

	// 3. Locate the Kubeconfig Secret in the Hub
	secretName := fmt.Sprintf("%s-admin-kubeconfig", hcName)
	secret := &corev1.Secret{}

	err = r.Get(ctx, types.NamespacedName{Name: secretName, Namespace: hcNamespace}, secret)
	if err != nil {
		if errors.IsNotFound(err) {
			logger.Info("Kubeconfig secret not found yet, requeuing...", "Secret", secretName)
			return ctrl.Result{RequeueAfter: time.Second * 10}, nil
		}
		return ctrl.Result{}, err
	}

	// 4. Build the connection to the Hosted Cluster
	// 4. Build the connection to the Hosted Cluster using the injected builder
	var hostedClient kubernetes.Interface
	if r.RemoteClientBuilder != nil {
		hostedClient, err = r.RemoteClientBuilder(secret.Data["kubeconfig"])
	} else {
		// Fallback to default behavior if not set
		hostedRESTConfig, err := clientcmd.RESTConfigFromKubeConfig(secret.Data["kubeconfig"])
		if err != nil {
			return ctrl.Result{}, err
		}
		hostedClient, err = kubernetes.NewForConfig(hostedRESTConfig)
	}
	if err != nil {
		return ctrl.Result{}, err
	}

	// 5. Connection Manager: Start Informer if it doesn't exist
	r.mu.Lock()
	_, exists := r.Connections[hcName]
	r.mu.Unlock()

	if !exists {
		ctxWatcher, cancel := context.WithCancel(context.Background())
		r.mu.Lock()
		r.Connections[hcName] = cancel
		r.mu.Unlock()

		logger.Info("🚀 Starting dynamic Informer for Hosted Cluster", "Cluster", hcName)
		go r.startHostedClusterWatcher(ctxWatcher, hostedClient, hcName, hcNamespace)
	}

	// 6. Fetch the target Service to get the NodePort
	targetSvc, err := hostedClient.CoreV1().Services(targetNs).Get(ctx, targetName, metav1.GetOptions{})
	if err != nil {
		if errors.IsNotFound(err) {
			logger.Info("Target service not found in Hosted Cluster yet, requeuing...")
			return ctrl.Result{RequeueAfter: time.Second * 10}, nil
		}
		return ctrl.Result{}, err
	}

	var actualNodePort int32
	for _, port := range targetSvc.Spec.Ports {
		if port.NodePort > 0 {
			actualNodePort = port.NodePort
			break
		}
	}

	if actualNodePort == 0 {
		logger.Info("Target service exists but has no NodePort assigned. Requeuing...")
		return ctrl.Result{RequeueAfter: time.Second * 10}, nil
	}

	// 7. Sync Hub Service targetPort to match the actual NodePort
	if len(hubSvc.Spec.Ports) > 0 && hubSvc.Spec.Ports[0].TargetPort.IntVal != actualNodePort {
		logger.Info("Updating Hub Service targetPort", "NewPort", actualNodePort)
		hubSvc.Spec.Ports[0].TargetPort = intstr.FromInt(int(actualNodePort))
		if err := r.Update(ctx, hubSvc); err != nil {
			return ctrl.Result{}, err
		}
		// Return and let the update event trigger the next reconcile
		return ctrl.Result{}, nil
	}

	// 8. ACTIVE NODE DETECTION LOGIC
	logger.Info("Discovering active nodes hosting the remote service pods...")
	labelSelector := fmt.Sprintf("kubernetes.io/service-name=%s", targetName)
	remoteSlices, err := hostedClient.DiscoveryV1().EndpointSlices(targetNs).List(ctx, metav1.ListOptions{
		LabelSelector: labelSelector,
	})
	if err != nil {
		return ctrl.Result{}, err
	}

	// Collect UNIQUE node names that have Ready pods
	activeNodeNames := make(map[string]bool)
	for _, slice := range remoteSlices.Items {
		for _, ep := range slice.Endpoints {
			// Skip pods that are not ready
			if ep.Conditions.Ready != nil && !*ep.Conditions.Ready {
				continue
			}
			if ep.NodeName != nil && *ep.NodeName != "" {
				activeNodeNames[*ep.NodeName] = true
			}
		}
	}

	// 9. Fetch IPs for the active nodes only
	var hubEndpoints []discoveryv1.Endpoint
	for nodeName := range activeNodeNames {
		node, err := hostedClient.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
		if err != nil {
			logger.Error(err, "Failed to fetch node details", "NodeName", nodeName)
			continue
		}

		var nodeIP string
		for _, addr := range node.Status.Addresses {
			if addr.Type == corev1.NodeInternalIP {
				nodeIP = addr.Address
				break
			}
		}

		if nodeIP != "" {
			hubEndpoints = append(hubEndpoints, discoveryv1.Endpoint{
				Addresses: []string{nodeIP},
				NodeName:  ptr.To(nodeName),
			})
		}
	}

	// 10. Create or Update EndpointSlice in the Hub
	expectedSlice := &discoveryv1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-shadow", hubSvc.Name),
			Namespace: hubSvc.Namespace,
		},
	}

	op, err := controllerutil.CreateOrUpdate(ctx, r.Client, expectedSlice, func() error {
		if err := controllerutil.SetControllerReference(hubSvc, expectedSlice, r.Scheme); err != nil {
			return err
		}

		if expectedSlice.Labels == nil {
			expectedSlice.Labels = make(map[string]string)
		}
		expectedSlice.Labels["kubernetes.io/service-name"] = hubSvc.Name
		expectedSlice.AddressType = discoveryv1.AddressTypeIPv4

		// EndpointSlice port must match the NodePort of the hosted cluster
		expectedSlice.Ports = []discoveryv1.EndpointPort{{
			Name: &hubSvc.Spec.Ports[0].Name,
			Port: ptr.To[int32](actualNodePort),
		}}

		expectedSlice.Endpoints = hubEndpoints
		return nil
	})

	if err != nil {
		logger.Error(err, "Failed to reconcile EndpointSlice")
		return ctrl.Result{}, err
	}

	if op != controllerutil.OperationResultNone {
		logger.Info("✅ EndpointSlice synced!", "Operation", string(op), "ActiveNodes", len(hubEndpoints))
	}

	return ctrl.Result{}, nil
}

// startHostedClusterWatcher starts informers for Services, Nodes, and EndpointSlices in the guest cluster
func (r *ServiceReconciler) startHostedClusterWatcher(ctx context.Context, hostedClient kubernetes.Interface, hcName, hcNamespace string) {
	factory := informers.NewSharedInformerFactory(hostedClient, time.Minute*10)

	// Watch Services for NodePort changes
	factory.Core().V1().Services().Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj interface{}) { r.enqueueSpecificHubService(obj, hcName, hcNamespace) },
		UpdateFunc: func(old, new interface{}) { r.enqueueSpecificHubService(new, hcName, hcNamespace) },
		DeleteFunc: func(obj interface{}) { r.enqueueSpecificHubService(obj, hcName, hcNamespace) },
	})

	// Watch EndpointSlices for Pod scaling/scheduling changes
	factory.Discovery().V1().EndpointSlices().Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj interface{}) { r.enqueueSpecificHubService(obj, hcName, hcNamespace) },
		UpdateFunc: func(old, new interface{}) { r.enqueueSpecificHubService(new, hcName, hcNamespace) },
		DeleteFunc: func(obj interface{}) { r.enqueueSpecificHubService(obj, hcName, hcNamespace) },
	})

	// Watch Nodes for Node IP changes
	factory.Core().V1().Nodes().Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj interface{}) { r.enqueueAllHubServicesForCluster(hcName, hcNamespace) },
		UpdateFunc: func(old, new interface{}) { r.enqueueAllHubServicesForCluster(hcName, hcNamespace) },
		DeleteFunc: func(obj interface{}) { r.enqueueAllHubServicesForCluster(hcName, hcNamespace) },
	})

	factory.Start(ctx.Done())
	factory.WaitForCacheSync(ctx.Done())

	<-ctx.Done()
}

// enqueueSpecificHubService wakes up the Hub Service associated with a specific Hosted Service event
func (r *ServiceReconciler) enqueueSpecificHubService(obj interface{}, hcName, hcNamespace string) {
	var remoteNs, remoteName string

	// Handle both Services and EndpointSlices
	switch v := obj.(type) {
	case *corev1.Service:
		remoteNs = v.Namespace
		remoteName = v.Name
	case *discoveryv1.EndpointSlice:
		remoteNs = v.Namespace
		// Extract the actual service name from the EndpointSlice label
		if svcName, ok := v.Labels["kubernetes.io/service-name"]; ok {
			remoteName = svcName
		} else {
			return
		}
	default:
		return
	}

	expectedHcAnno := fmt.Sprintf("%s/%s", hcNamespace, hcName)
	expectedTargetAnno := fmt.Sprintf("%s/%s", remoteNs, remoteName)

	var hubServices corev1.ServiceList
	if err := r.List(context.Background(), &hubServices); err == nil {
		for _, hubSvc := range hubServices.Items {
			if hubSvc.Annotations[AnnoHostedCluster] == expectedHcAnno &&
				hubSvc.Annotations[AnnoTargetService] == expectedTargetAnno {
				r.TriggerChannel <- event.GenericEvent{Object: hubSvc.DeepCopy()}
			}
		}
	}
}

// enqueueAllHubServicesForCluster wakes up all Hub Services linked to a specific Hosted Cluster (useful for Node updates)
func (r *ServiceReconciler) enqueueAllHubServicesForCluster(hcName, hcNamespace string) {
	expectedHcAnno := fmt.Sprintf("%s/%s", hcNamespace, hcName)
	var hubServices corev1.ServiceList
	if err := r.List(context.Background(), &hubServices); err == nil {
		for _, hubSvc := range hubServices.Items {
			if hubSvc.Annotations[AnnoHostedCluster] == expectedHcAnno {
				r.TriggerChannel <- event.GenericEvent{Object: hubSvc.DeepCopy()}
			}
		}
	}
}

// SetupWithManager sets up the controller with the Manager.
func (r *ServiceReconciler) SetupWithManager(mgr ctrl.Manager) error {
	r.Connections = make(map[string]context.CancelFunc)
	r.TriggerChannel = make(chan event.GenericEvent, 1024)

	// Filter events to only reconcile Services with our specific annotation
	annotationFilter := predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool {
			_, ok := e.Object.GetAnnotations()[AnnoHostedCluster]
			return ok
		},
		UpdateFunc: func(e event.UpdateEvent) bool {
			_, okNew := e.ObjectNew.GetAnnotations()[AnnoHostedCluster]
			_, okOld := e.ObjectOld.GetAnnotations()[AnnoHostedCluster]
			return okNew || okOld
		},
		DeleteFunc: func(e event.DeleteEvent) bool {
			_, ok := e.Object.GetAnnotations()[AnnoHostedCluster]
			return ok
		},
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&corev1.Service{}).
		WithEventFilter(annotationFilter).
		WatchesRawSource(source.Channel(r.TriggerChannel, &handler.EnqueueRequestForObject{})).
		Complete(r)
}
