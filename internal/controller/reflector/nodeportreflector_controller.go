package reflector

import (
	"context"
	"fmt"
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

	reflectorv1alpha1 "github.com/bizcochillo/hcp-nodeport-reflector/api/reflector/v1alpha1"
)

const (
	ModeNodes = "nodes" // Default: Pure Reflector mode (targets all worker nodes)
	ModePods  = "pods"  // Target strictly worker nodes hosting active/ready pods
)

type RemoteClientBuilderFunc func(kubeconfigBytes []byte) (kubernetes.Interface, error)

type NodePortReflectorReconciler struct {
	client.Client
	Scheme              *runtime.Scheme
	RemoteClientBuilder RemoteClientBuilderFunc

	mu             sync.Mutex
	Watchers       map[string]context.CancelFunc
	Subscribers    map[string]map[types.NamespacedName]bool
	TriggerChannel chan event.GenericEvent
}

// +kubebuilder:rbac:groups=reflector.bizcochillo.io,resources=nodeportreflectors,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=reflector.bizcochillo.io,resources=nodeportreflectors/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=reflector.bizcochillo.io,resources=nodeportreflectors/finalizers,verbs=update
// +kubebuilder:rbac:groups=core,resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=secrets,verbs=get;list;watch
// +kubebuilder:rbac:groups=discovery.k8s.io,resources=endpointslices,verbs=get;list;watch;create;update;patch;delete

func (r *NodePortReflectorReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	// 1. Fetch CRD
	npr := &reflectorv1alpha1.NodePortReflector{}
	if err := r.Get(ctx, req.NamespacedName, npr); err != nil {
		if apierrors.IsNotFound(err) {
			r.removeSubscriber(req.NamespacedName)
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	clusterNS := npr.Spec.HostedCluster.Namespace
	if clusterNS == "" {
		clusterNS = npr.Namespace
	}
	clusterName := npr.Spec.HostedCluster.Name
	targetNS := npr.Spec.TargetService.Namespace
	if targetNS == "" {
		targetNS = npr.Namespace
	}
	targetServiceName := npr.Spec.TargetService.Name

	// 2. Register Subscriber & Ensure Remote Watcher
	hostedClient, err := r.getHostedClient(ctx, clusterNS, clusterName)
	if err != nil {
		logger.Error(err, "Failed to get remote client")
		_ = r.updateStatus(ctx, npr, "Error", "ClientNotFound", err.Error(), 0, nil, nil)
		return ctrl.Result{}, err
	}
	r.ensureWatcher(ctx, req.NamespacedName, clusterName, hostedClient)

	// 3. Fetch Remote Service (Detect Policy & Ports)
	remoteSvc, err := hostedClient.CoreV1().Services(targetNS).Get(ctx, targetServiceName, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			logger.Info("⚠️ Remote service not found, clearing Hub endpoints")

			// Vaciamos explícitamente solo los Endpoints (mantenemos el Service intacto por seguridad)
			sliceName := fmt.Sprintf("%s-shadow", npr.Name)
			existingSlice := &discoveryv1.EndpointSlice{}
			if getErr := r.Get(ctx, types.NamespacedName{Name: sliceName, Namespace: npr.Namespace}, existingSlice); getErr == nil {
				existingSlice.Endpoints = []discoveryv1.Endpoint{}
				_ = r.Update(ctx, existingSlice)
			}

			_ = r.updateStatus(ctx, npr, "Pending", "RemoteServiceMissing", "Target service not found in Hosted Cluster", 0, nil, nil)
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	// 4. Infer Mode from ExternalTrafficPolicy
	mode := ModeNodes
	policyStr := string(corev1.ServiceExternalTrafficPolicyCluster)
	if remoteSvc.Spec.ExternalTrafficPolicy == corev1.ServiceExternalTrafficPolicyLocal {
		mode = ModePods
		policyStr = string(corev1.ServiceExternalTrafficPolicyLocal)
	}

	// 5. Discover Node IPs
	activeNodeIPs, err := r.getActiveNodeIPs(ctx, hostedClient, targetNS, targetServiceName, mode)
	if err != nil {
		logger.Error(err, "Failed to discover nodes")
		return ctrl.Result{}, err
	}

	// 6. Sync Hub Resources
	if err := r.syncHubResources(ctx, npr, remoteSvc.Spec.Ports, activeNodeIPs); err != nil {
		return ctrl.Result{}, err
	}

	// 7. Update Status
	err = r.updateStatus(ctx, npr, "Synced", "Synced", "Successfully reflected service", len(activeNodeIPs), remoteSvc.Spec.Ports, &policyStr)
	return ctrl.Result{}, err
}

func (r *NodePortReflectorReconciler) syncHubResources(ctx context.Context, npr *reflectorv1alpha1.NodePortReflector, remotePorts []corev1.ServicePort, activeNodeIPs []string) error {
	// A) Sync Service
	hubSvc := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: npr.Name, Namespace: npr.Namespace}}
	var svcPorts []corev1.ServicePort
	for _, rp := range remotePorts {
		if rp.NodePort != 0 {
			svcPorts = append(svcPorts, corev1.ServicePort{
				Name:       rp.Name,
				Port:       rp.Port,
				TargetPort: intstr.FromInt32(rp.NodePort),
				Protocol:   rp.Protocol,
			})
		}
	}

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, hubSvc, func() error {
		if hubSvc.Labels == nil {
			hubSvc.Labels = map[string]string{}
		}
		hubSvc.Labels["app.kubernetes.io/managed-by"] = "hcp-nodeport-reflector"
		hubSvc.Spec.Type = corev1.ServiceTypeClusterIP
		hubSvc.Spec.Ports = svcPorts
		return controllerutil.SetControllerReference(npr, hubSvc, r.Scheme)
	})
	if err != nil {
		return err
	}

	// B) Sync EndpointSlice
	sliceName := fmt.Sprintf("%s-shadow", npr.Name)
	expectedSlice := &discoveryv1.EndpointSlice{ObjectMeta: metav1.ObjectMeta{Name: sliceName, Namespace: npr.Namespace}}

	_, err = controllerutil.CreateOrUpdate(ctx, r.Client, expectedSlice, func() error {
		if expectedSlice.Labels == nil {
			expectedSlice.Labels = map[string]string{}
		}
		expectedSlice.Labels["kubernetes.io/service-name"] = npr.Name
		expectedSlice.Labels["endpointslice.kubernetes.io/managed-by"] = "hcp-nodeport-reflector"
		expectedSlice.AddressType = discoveryv1.AddressTypeIPv4

		var slicePorts []discoveryv1.EndpointPort
		for _, rp := range remotePorts {
			if rp.NodePort != 0 {
				portName := rp.Name
				proto := rp.Protocol
				slicePorts = append(slicePorts, discoveryv1.EndpointPort{
					Name:     &portName,
					Port:     ptr.To(rp.NodePort),
					Protocol: &proto,
				})
			}
		}
		expectedSlice.Ports = slicePorts

		if len(activeNodeIPs) > 0 {
			expectedSlice.Endpoints = []discoveryv1.Endpoint{
				{Addresses: activeNodeIPs, Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true)}},
			}
		} else {
			expectedSlice.Endpoints = []discoveryv1.Endpoint{}
		}

		return controllerutil.SetControllerReference(npr, expectedSlice, r.Scheme)
	})

	return err
}

func (r *NodePortReflectorReconciler) getActiveNodeIPs(ctx context.Context, hostedClient kubernetes.Interface, namespace, serviceName, mode string) ([]string, error) {
	podNodeMap := make(map[string]bool)

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
		if len(podNodeMap) == 0 {
			return []string{}, nil
		}
	}

	nodes, err := hostedClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}

	var nodeIPs []string
	for _, node := range nodes.Items {
		if isControlPlaneNode(node) {
			continue
		}
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

func isControlPlaneNode(node corev1.Node) bool {
	_, isControlPlane := node.Labels["node-role.kubernetes.io/control-plane"]
	_, isMaster := node.Labels["node-role.kubernetes.io/master"]
	return isControlPlane || isMaster
}

func (r *NodePortReflectorReconciler) getHostedClient(ctx context.Context, namespace, clusterName string) (kubernetes.Interface, error) {
	candidates := []types.NamespacedName{
		{Namespace: namespace, Name: fmt.Sprintf("%s-admin-kubeconfig", clusterName)},
		{Namespace: namespace, Name: fmt.Sprintf("%s-cluster-secret", clusterName)},
		{Namespace: fmt.Sprintf("clusters-%s", clusterName), Name: "admin-kubeconfig"},
		{Namespace: clusterName, Name: "admin-kubeconfig"},
	}

	var secret corev1.Secret
	var found bool
	for _, loc := range candidates {
		if err := r.Get(ctx, loc, &secret); err == nil {
			found = true
			break
		}
	}

	if !found {
		return nil, fmt.Errorf("failed to find kubeconfig secret")
	}

	kubeconfigBytes, ok := secret.Data["kubeconfig"]
	if !ok {
		kubeconfigBytes, ok = secret.Data["value"]
	}
	if !ok {
		return nil, fmt.Errorf("secret missing 'kubeconfig' or 'value'")
	}

	if r.RemoteClientBuilder != nil {
		return r.RemoteClientBuilder(kubeconfigBytes)
	}

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

func (r *NodePortReflectorReconciler) updateStatus(ctx context.Context, npr *reflectorv1alpha1.NodePortReflector, phase, reason, message string, activeNodes int, ports []corev1.ServicePort, policy *string) error {
	npr.Status.Phase = phase
	if policy != nil {
		npr.Status.DetectedPolicy = *policy
	}
	npr.Status.ActiveNodes = activeNodes

	syncedPorts := make([]reflectorv1alpha1.PortStatus, 0, len(ports))
	for _, p := range ports {
		syncedPorts = append(syncedPorts, reflectorv1alpha1.PortStatus{
			Name:     p.Name,
			Port:     p.Port,
			NodePort: p.NodePort,
			Protocol: string(p.Protocol),
		})
	}
	npr.Status.SyncedPorts = syncedPorts

	cond := metav1.Condition{
		Type:               "Ready",
		Status:             metav1.ConditionTrue,
		Reason:             reason,
		Message:            message,
		LastTransitionTime: metav1.Now(),
	}
	if phase != "Synced" {
		cond.Status = metav1.ConditionFalse
	}

	found := false
	for i, c := range npr.Status.Conditions {
		if c.Type == cond.Type {
			npr.Status.Conditions[i] = cond
			found = true
			break
		}
	}
	if !found {
		npr.Status.Conditions = append(npr.Status.Conditions, cond)
	}

	return r.Status().Update(ctx, npr)
}

// ensureWatcher sets up a dynamic Informer on the remote cluster for Services and EndpointSlices
func (r *NodePortReflectorReconciler) ensureWatcher(ctx context.Context, nprKey types.NamespacedName, clusterName string, hostedClient kubernetes.Interface) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.Subscribers == nil {
		r.Subscribers = make(map[string]map[types.NamespacedName]bool)
	}
	if r.Subscribers[clusterName] == nil {
		r.Subscribers[clusterName] = make(map[types.NamespacedName]bool)
	}
	r.Subscribers[clusterName][nprKey] = true

	if r.Watchers == nil {
		r.Watchers = make(map[string]context.CancelFunc)
	}

	if _, exists := r.Watchers[clusterName]; exists {
		return
	}

	log.FromContext(ctx).Info("🚀 Starting Dual Informer for Hosted Cluster", "Cluster", clusterName)

	watchCtx, cancel := context.WithCancel(context.Background())
	r.Watchers[clusterName] = cancel

	factory := informers.NewSharedInformerFactory(hostedClient, 10*time.Minute)

	// Dual Handlers
	handlerFuncs := cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj any) { r.triggerAll(clusterName) },
		UpdateFunc: func(oldObj, newObj any) { r.triggerAll(clusterName) },
		DeleteFunc: func(obj any) { r.triggerAll(clusterName) },
	}

	_, _ = factory.Discovery().V1().EndpointSlices().Informer().AddEventHandler(handlerFuncs)
	_, _ = factory.Core().V1().Services().Informer().AddEventHandler(handlerFuncs)

	factory.Start(watchCtx.Done())
}

func (r *NodePortReflectorReconciler) triggerAll(clusterName string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.TriggerChannel == nil {
		return
	}
	for nprKey := range r.Subscribers[clusterName] {
		r.TriggerChannel <- event.GenericEvent{
			Object: &reflectorv1alpha1.NodePortReflector{
				ObjectMeta: metav1.ObjectMeta{
					Name:      nprKey.Name,
					Namespace: nprKey.Namespace,
				},
			},
		}
	}
}

func (r *NodePortReflectorReconciler) removeSubscriber(nprKey types.NamespacedName) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for cluster, subs := range r.Subscribers {
		delete(subs, nprKey)
		if len(subs) == 0 {
			if cancel, exists := r.Watchers[cluster]; exists {
				cancel()
				delete(r.Watchers, cluster)
			}
			delete(r.Subscribers, cluster)
		}
	}
}

// SetupWithManager registers the controller
func (r *NodePortReflectorReconciler) SetupWithManager(mgr ctrl.Manager) error {
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
		For(&reflectorv1alpha1.NodePortReflector{}).
		Owns(&corev1.Service{}).
		Owns(&discoveryv1.EndpointSlice{}).
		WatchesRawSource(
			source.Channel(r.TriggerChannel, &handler.EnqueueRequestForObject{}),
		).
		Complete(r)
}
