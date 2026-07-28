package controller

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
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
	"sigs.k8s.io/controller-runtime/pkg/source"
)

const (
	AnnoHostedCluster = "reflector.bizcochillo.io/hosted-cluster"
	AnnoTargetService = "reflector.bizcochillo.io/target-service"
	AnnoMode          = "reflector.bizcochillo.io/mode"

	ModeNodes = "nodes" // Default: Pure Reflector mode (targets all worker nodes)
	ModePods  = "pods"  // Target strictly worker nodes hosting active/ready pods
)

// RemoteClientBuilderFunc defines the signature for client builder dependency injection
type RemoteClientBuilderFunc func(kubeconfigBytes []byte) (kubernetes.Interface, error)

// ServiceReconciler reconciles a Service object in the Hub
type ServiceReconciler struct {
	client.Client
	Scheme              *runtime.Scheme
	RemoteClientBuilder RemoteClientBuilderFunc

	mu             sync.Mutex
	Connections    map[string]context.CancelFunc
	TriggerChannel chan event.GenericEvent
}

// +kubebuilder:rbac:groups=core,resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=services/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=core,resources=services/finalizers,verbs=update;patch
// +kubebuilder:rbac:groups=core,resources=secrets,verbs=get;list;watch
// +kubebuilder:rbac:groups=discovery.k8s.io,resources=endpointslices,verbs=get;list;watch;create;update;patch;delete

func (r *ServiceReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	// 1. Fetch the Hub Service
	hubSvc := &corev1.Service{}
	if err := r.Get(ctx, req.NamespacedName, hubSvc); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	// 2. Validate Annotations
	clusterRef, hasCluster := hubSvc.Annotations[AnnoHostedCluster]
	targetRef, hasTarget := hubSvc.Annotations[AnnoTargetService]

	if !hasCluster || !hasTarget {
		return ctrl.Result{}, nil
	}

	clusterParts := strings.Split(clusterRef, "/")
	targetParts := strings.Split(targetRef, "/")

	if len(clusterParts) != 2 || len(targetParts) != 2 {
		logger.Error(fmt.Errorf("invalid annotation format"), "Annotations must be namespace/name")
		return ctrl.Result{}, nil
	}

	clusterNamespace, clusterName := clusterParts[0], clusterParts[1]
	targetNamespace, targetServiceName := targetParts[0], targetParts[1]

	// Parse routing mode (defaults to ModeNodes)
	mode := hubSvc.Annotations[AnnoMode]
	if mode != ModePods {
		mode = ModeNodes
	}

	// 3. Obtain Guest Cluster Client
	hostedClient, err := r.getHostedClient(ctx, clusterNamespace, clusterName)
	if err != nil {
		logger.Error(err, "Failed to build client for Hosted Cluster", "Cluster", clusterName)
		return ctrl.Result{}, err
	}

	// 4. Ensure Dynamic Watcher is Active for this Cluster
	r.ensureWatcher(ctx, clusterName, hostedClient)

	// 5. Fetch Remote NodePort Service to align Ports
	remoteSvc, err := hostedClient.CoreV1().Services(targetNamespace).Get(ctx, targetServiceName, metav1.GetOptions{})
	var targetNodePort int32 = 0

	if err != nil {
		if !apierrors.IsNotFound(err) {
			logger.Error(err, "Failed to get target service in Hosted Cluster")
			return ctrl.Result{}, err
		}
		logger.Info("⚠️ Remote service not found in Hosted Cluster", "Target", targetServiceName)
	} else if len(remoteSvc.Spec.Ports) > 0 {
		targetNodePort = remoteSvc.Spec.Ports[0].NodePort
	}

	// 6. Sync Hub Service TargetPort if NodePort changed
	if targetNodePort != 0 && (len(hubSvc.Spec.Ports) == 0 || hubSvc.Spec.Ports[0].TargetPort.IntVal != targetNodePort) {
		logger.Info("Updating Hub Service targetPort", "NewPort", targetNodePort)
		if len(hubSvc.Spec.Ports) == 0 {
			hubSvc.Spec.Ports = []corev1.ServicePort{{Name: "http", Port: 80}}
		}
		hubSvc.Spec.Ports[0].TargetPort = intstr.FromInt32(targetNodePort)
		if err := r.Update(ctx, hubSvc); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	// 7. Discover Active Nodes based on configured Mode ("nodes" vs "pods")
	logger.Info("Discovering active nodes for remote service...", "Mode", mode)
	activeNodeIPs, err := r.getActiveNodeIPs(ctx, hostedClient, targetNamespace, targetServiceName, mode)
	if err != nil {
		if apierrors.IsNotFound(err) {
			logger.Info("⚠️ Remote endpoints/service missing in Hosted Cluster, clearing Hub EndpointSlice")
			activeNodeIPs = []string{}
		} else {
			logger.Error(err, "Failed to discover active nodes")
			return ctrl.Result{}, err
		}
	}

	// 8. Construct expected Hub EndpointSlice
	sliceName := fmt.Sprintf("%s-shadow", hubSvc.Name)
	expectedSlice := &discoveryv1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Name:      sliceName,
			Namespace: hubSvc.Namespace,
			Labels: map[string]string{
				"kubernetes.io/service-name":             hubSvc.Name,
				"endpointslice.kubernetes.io/managed-by": "hcp-nodeport-reflector",
			},
		},
		AddressType: discoveryv1.AddressTypeIPv4,
	}

	// Set OwnerReference for Automatic Garbage Collection
	if err := controllerutil.SetControllerReference(hubSvc, expectedSlice, r.Scheme); err != nil {
		return ctrl.Result{}, err
	}

	// Populate Ports
	if targetNodePort != 0 {
		expectedSlice.Ports = []discoveryv1.EndpointPort{
			{
				Name:     ptr.To("http"),
				Port:     ptr.To(targetNodePort),
				Protocol: ptr.To(corev1.ProtocolTCP),
			},
		}
	}

	// Populate Endpoints
	if len(activeNodeIPs) > 0 {
		expectedSlice.Endpoints = []discoveryv1.Endpoint{
			{
				Addresses: activeNodeIPs,
				Conditions: discoveryv1.EndpointConditions{
					Ready: ptr.To(true),
				},
			},
		}
	} else {
		expectedSlice.Endpoints = []discoveryv1.Endpoint{}
	}

	// 9. Mutate or Create EndpointSlice in Hub (with No-Op change detection)
	existingSlice := &discoveryv1.EndpointSlice{}
	err = r.Get(ctx, types.NamespacedName{Name: sliceName, Namespace: hubSvc.Namespace}, existingSlice)

	if apierrors.IsNotFound(err) {
		if err := r.Create(ctx, expectedSlice); err != nil {
			return ctrl.Result{}, err
		}
		logger.Info("✅ EndpointSlice synced!", "Operation", "created", "ActiveNodes", len(activeNodeIPs), "Mode", mode)
	} else if err == nil {
		// PREVENCIÓN DE BUCLE: Solo actualizamos si hay cambios reales en la especificación
		if !reflect.DeepEqual(existingSlice.Endpoints, expectedSlice.Endpoints) ||
			!reflect.DeepEqual(existingSlice.Ports, expectedSlice.Ports) ||
			!reflect.DeepEqual(existingSlice.Labels, expectedSlice.Labels) {

			existingSlice.Endpoints = expectedSlice.Endpoints
			existingSlice.Ports = expectedSlice.Ports
			existingSlice.Labels = expectedSlice.Labels

			if err := r.Update(ctx, existingSlice); err != nil {
				return ctrl.Result{}, err
			}
			logger.Info("✅ EndpointSlice synced!", "Operation", "updated", "ActiveNodes", len(activeNodeIPs), "Mode", mode)
		}
	} else {
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

// getActiveNodeIPs fetches Node IPs based on Mode:
// - "nodes" (default): Reflects ALL active worker nodes directly (Pure Reflector).
// - "pods": Filters strictly by worker nodes actively hosting ready pods.
func (r *ServiceReconciler) getActiveNodeIPs(ctx context.Context, hostedClient kubernetes.Interface, namespace, serviceName, mode string) ([]string, error) {
	podNodeMap := make(map[string]bool)

	// In "pods" mode, we query EndpointSlices to filter down to specific host nodes
	if mode == ModePods {
		labelSelector := fmt.Sprintf("kubernetes.io/service-name=%s", serviceName)
		slices, err := hostedClient.DiscoveryV1().EndpointSlices(namespace).List(ctx, metav1.ListOptions{LabelSelector: labelSelector})
		if err != nil {
			return nil, err
		}

		for _, slice := range slices.Items {
			for _, ep := range slice.Endpoints {
				if ep.Conditions.Ready != nil && *ep.Conditions.Ready {
					if ep.NodeName != nil && *ep.NodeName != "" {
						podNodeMap[*ep.NodeName] = true
					}
				}
			}
		}

		// If mode is "pods" and no nodes host ready pods, return empty
		if len(podNodeMap) == 0 {
			return []string{}, nil
		}
	}

	// Fetch all cluster worker nodes
	nodes, err := hostedClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}

	var nodeIPs []string
	for _, node := range nodes.Items {
		// Filter out Control-Plane / Master nodes
		if isControlPlaneNode(node) {
			continue
		}

		// In "pods" mode, skip nodes without ready pods
		if mode == ModePods && !podNodeMap[node.Name] {
			continue
		}

		for _, addr := range node.Status.Addresses {
			if addr.Type == corev1.NodeInternalIP {
				nodeIPs = append(nodeIPs, addr.Address)
				break
			}
		}
	}

	return nodeIPs, nil
}

// isControlPlaneNode identifies control-plane/master nodes to exclude them
func isControlPlaneNode(node corev1.Node) bool {
	_, isControlPlane := node.Labels["node-role.kubernetes.io/control-plane"]
	_, isMaster := node.Labels["node-role.kubernetes.io/master"]
	return isControlPlane || isMaster
}

// getHostedClient retrieves the admin kubeconfig Secret and builds the clientset
func (r *ServiceReconciler) getHostedClient(ctx context.Context, namespace, clusterName string) (kubernetes.Interface, error) {
	secretName := fmt.Sprintf("%s-admin-kubeconfig", clusterName)
	secret := &corev1.Secret{}

	if err := r.Get(ctx, types.NamespacedName{Name: secretName, Namespace: namespace}, secret); err != nil {
		return nil, fmt.Errorf("failed to find kubeconfig secret %s/%s: %w", namespace, secretName, err)
	}

	kubeconfigBytes, ok := secret.Data["kubeconfig"]
	if !ok {
		return nil, fmt.Errorf("secret %s/%s missing 'kubeconfig' key", namespace, secretName)
	}

	if r.RemoteClientBuilder != nil {
		return r.RemoteClientBuilder(kubeconfigBytes)
	}

	clientConfig, err := clientcmd.NewClientConfigFromBytes(kubeconfigBytes)
	if err != nil {
		return nil, fmt.Errorf("invalid kubeconfig bytes: %w", err)
	}

	restConfig, err := clientConfig.ClientConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to build REST config: %w", err)
	}

	return kubernetes.NewForConfig(restConfig)
}

// ensureWatcher sets up a dynamic Informer on the remote cluster to trigger Hub reconciliations
func (r *ServiceReconciler) ensureWatcher(ctx context.Context, clusterName string, hostedClient kubernetes.Interface) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.Connections == nil {
		r.Connections = make(map[string]context.CancelFunc)
	}

	if _, exists := r.Connections[clusterName]; exists {
		return
	}

	logger := log.FromContext(ctx)
	logger.Info("🚀 Starting dynamic Informer for Hosted Cluster", "Cluster", clusterName)

	watchCtx, cancel := context.WithCancel(context.Background())
	r.Connections[clusterName] = cancel

	factory := informers.NewSharedInformerFactory(hostedClient, 10*time.Minute)
	endpointSliceInformer := factory.Discovery().V1().EndpointSlices().Informer()

	extractServiceName := func(obj interface{}) string {
		if slice, ok := obj.(*discoveryv1.EndpointSlice); ok {
			return slice.Labels["kubernetes.io/service-name"]
		}
		if tombstone, ok := obj.(cache.DeletedFinalStateUnknown); ok {
			if slice, ok := tombstone.Obj.(*discoveryv1.EndpointSlice); ok {
				return slice.Labels["kubernetes.io/service-name"]
			}
		}
		return ""
	}

	handlerFuncs := cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			if svcName := extractServiceName(obj); svcName != "" {
				r.triggerReconcile(clusterName)
			}
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			if svcName := extractServiceName(newObj); svcName != "" {
				r.triggerReconcile(clusterName)
			}
		},
		DeleteFunc: func(obj interface{}) {
			if svcName := extractServiceName(obj); svcName != "" {
				r.triggerReconcile(clusterName)
			}
		},
	}

	_, _ = endpointSliceInformer.AddEventHandler(handlerFuncs)
	factory.Start(watchCtx.Done())
}

// triggerReconcile queues an event into the controller-runtime channel
func (r *ServiceReconciler) triggerReconcile(clusterName string) {
	if r.TriggerChannel != nil {
		r.TriggerChannel <- event.GenericEvent{
			Object: &corev1.Service{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "trigger-" + clusterName,
					Namespace: "default",
				},
			},
		}
	}
}

// SetupWithManager registers the controller with the manager
func (r *ServiceReconciler) SetupWithManager(mgr ctrl.Manager) error {
	r.Connections = make(map[string]context.CancelFunc)
	r.TriggerChannel = make(chan event.GenericEvent, 1024)

	if r.RemoteClientBuilder == nil {
		r.RemoteClientBuilder = func(kubeconfigBytes []byte) (kubernetes.Interface, error) {
			clientConfig, err := clientcmd.NewClientConfigFromBytes(kubeconfigBytes)
			if err != nil {
				return nil, err
			}
			restConfig, err := clientConfig.ClientConfig()
			if err != nil {
				return nil, err
			}
			return kubernetes.NewForConfig(restConfig)
		}
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&corev1.Service{}).
		Owns(&discoveryv1.EndpointSlice{}).
		WatchesRawSource(
			source.Channel(r.TriggerChannel, &handler.EnqueueRequestForObject{}),
		).
		Complete(r)
}
