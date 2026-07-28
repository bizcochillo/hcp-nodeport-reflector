package controller

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

var _ = Describe("Service Controller - Multi-Cluster Reflector Suite", func() {
	const (
		ServiceNamespace = "default"
		ClusterName      = "mock-cluster"
		TargetSvcName    = "remote-app"
		TargetNodePort   = int32(32222)
	)

	var (
		reconciler   *ServiceReconciler
		fakeClient   *fake.Clientset
		secretObject *corev1.Secret
	)

	BeforeEach(func() {
		// 1. Setup Fake Remote Cluster Client
		fakeClient = fake.NewSimpleClientset(
			&corev1.Node{
				ObjectMeta: metav1.ObjectMeta{Name: "worker-1"},
				Status: corev1.NodeStatus{
					Addresses: []corev1.NodeAddress{{Type: corev1.NodeInternalIP, Address: "10.0.0.1"}},
				},
			},
			&corev1.Node{
				ObjectMeta: metav1.ObjectMeta{Name: "worker-2"},
				Status: corev1.NodeStatus{
					Addresses: []corev1.NodeAddress{{Type: corev1.NodeInternalIP, Address: "10.0.0.2"}},
				},
			},
			&corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:   "master-1",
					Labels: map[string]string{"node-role.kubernetes.io/control-plane": ""},
				},
				Status: corev1.NodeStatus{
					Addresses: []corev1.NodeAddress{{Type: corev1.NodeInternalIP, Address: "10.0.0.99"}},
				},
			},
			// Remote Target Service
			&corev1.Service{
				ObjectMeta: metav1.ObjectMeta{Name: TargetSvcName, Namespace: ServiceNamespace},
				Spec: corev1.ServiceSpec{
					Type: corev1.ServiceTypeNodePort,
					Ports: []corev1.ServicePort{
						{Name: PortNameHTTP, Port: 80, NodePort: TargetNodePort},
					},
				},
			},
			// Remote EndpointSlice with Ready Pod ONLY on worker-1
			&discoveryv1.EndpointSlice{
				ObjectMeta: metav1.ObjectMeta{
					Name:      TargetSvcName + "-slice",
					Namespace: ServiceNamespace,
					Labels:    map[string]string{"kubernetes.io/service-name": TargetSvcName},
				},
				Endpoints: []discoveryv1.Endpoint{
					{
						Addresses:  []string{"10.244.0.5"},
						Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true)},
						NodeName:   ptr.To("worker-1"),
					},
				},
			},
		)

		// 2. Setup Reconciler
		reconciler = &ServiceReconciler{
			Client: k8sClient,
			Scheme: k8sClient.Scheme(),
			RemoteClientBuilder: func(kubeconfigBytes []byte) (kubernetes.Interface, error) {
				return fakeClient, nil
			},
		}

		// 3. Create Required Secret in Hub
		secretObject = &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: ClusterName + "-admin-kubeconfig", Namespace: ServiceNamespace},
			Data:       map[string][]byte{"kubeconfig": []byte("dummy-kubeconfig-data")},
		}
		_ = k8sClient.Create(ctx, secretObject)
	})

	AfterEach(func() {
		_ = k8sClient.Delete(ctx, secretObject)
	})

	Context("Happy Path - Routing Modes", func() {
		It("Should include ALL worker nodes when mode is default ('nodes')", func() {
			svcName := "svc-nodes-mode"
			req := reconcile.Request{NamespacedName: types.NamespacedName{Name: svcName, Namespace: ServiceNamespace}}

			hubService := &corev1.Service{
				ObjectMeta: metav1.ObjectMeta{
					Name:      svcName,
					Namespace: ServiceNamespace,
					Annotations: map[string]string{
						AnnoHostedCluster: ServiceNamespace + "/" + ClusterName,
						AnnoTargetService: ServiceNamespace + "/" + TargetSvcName,
					},
				},
				Spec: corev1.ServiceSpec{
					Ports: []corev1.ServicePort{{Name: PortNameHTTP, Port: 80}},
				},
			}
			Expect(k8sClient.Create(ctx, hubService)).To(Succeed())
			defer func() { _ = k8sClient.Delete(ctx, hubService) }()

			// Pass 1: Sync TargetPort
			_, err := reconciler.Reconcile(ctx, req)
			Expect(err).NotTo(HaveOccurred())

			// Pass 2: Sync EndpointSlice
			_, err = reconciler.Reconcile(ctx, req)
			Expect(err).NotTo(HaveOccurred())

			slice := &discoveryv1.EndpointSlice{}
			err = k8sClient.Get(ctx, types.NamespacedName{Name: svcName + "-shadow", Namespace: ServiceNamespace}, slice)
			Expect(err).NotTo(HaveOccurred())
			Expect(slice.Endpoints).To(HaveLen(1))
			Expect(slice.Endpoints[0].Addresses).To(ConsistOf("10.0.0.1", "10.0.0.2"))
		})

		It("Should filter strictly by pod host node when mode is 'pods'", func() {
			svcName := "svc-pods-mode"
			req := reconcile.Request{NamespacedName: types.NamespacedName{Name: svcName, Namespace: ServiceNamespace}}

			hubService := &corev1.Service{
				ObjectMeta: metav1.ObjectMeta{
					Name:      svcName,
					Namespace: ServiceNamespace,
					Annotations: map[string]string{
						AnnoHostedCluster: ServiceNamespace + "/" + ClusterName,
						AnnoTargetService: ServiceNamespace + "/" + TargetSvcName,
						AnnoMode:          "pods",
					},
				},
				Spec: corev1.ServiceSpec{
					Ports: []corev1.ServicePort{{Name: PortNameHTTP, Port: 80}},
				},
			}
			Expect(k8sClient.Create(ctx, hubService)).To(Succeed())
			defer func() { _ = k8sClient.Delete(ctx, hubService) }()

			_, err := reconciler.Reconcile(ctx, req)
			Expect(err).NotTo(HaveOccurred())
			_, err = reconciler.Reconcile(ctx, req)
			Expect(err).NotTo(HaveOccurred())

			slice := &discoveryv1.EndpointSlice{}
			err = k8sClient.Get(ctx, types.NamespacedName{Name: svcName + "-shadow", Namespace: ServiceNamespace}, slice)
			Expect(err).NotTo(HaveOccurred())
			Expect(slice.Endpoints).To(HaveLen(1))
			Expect(slice.Endpoints[0].Addresses).To(Equal([]string{"10.0.0.1"}))
		})
	})

	Context("Edge Cases & Resilience", func() {
		It("Should ignore Services without reflector annotations", func() {
			svcName := "svc-no-annotations"
			req := reconcile.Request{NamespacedName: types.NamespacedName{Name: svcName, Namespace: ServiceNamespace}}

			hubService := &corev1.Service{
				ObjectMeta: metav1.ObjectMeta{Name: svcName, Namespace: ServiceNamespace},
				Spec: corev1.ServiceSpec{
					Ports: []corev1.ServicePort{{Name: PortNameHTTP, Port: 80}},
				},
			}
			Expect(k8sClient.Create(ctx, hubService)).To(Succeed())
			defer func() { _ = k8sClient.Delete(ctx, hubService) }()

			result, err := reconciler.Reconcile(ctx, req)
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(ctrl.Result{}))

			// EndpointSlice must NOT exist for this service
			slice := &discoveryv1.EndpointSlice{}
			err = k8sClient.Get(ctx, types.NamespacedName{Name: svcName + "-shadow", Namespace: ServiceNamespace}, slice)
			Expect(apierrors.IsNotFound(err)).To(BeTrue())
		})

		It("Should handle missing Secret gracefully by returning an error", func() {
			svcName := "svc-missing-secret"
			req := reconcile.Request{NamespacedName: types.NamespacedName{Name: svcName, Namespace: ServiceNamespace}}

			// Delete secret to simulate missing credentials
			Expect(k8sClient.Delete(ctx, secretObject)).To(Succeed())

			hubService := &corev1.Service{
				ObjectMeta: metav1.ObjectMeta{
					Name:      svcName,
					Namespace: ServiceNamespace,
					Annotations: map[string]string{
						AnnoHostedCluster: ServiceNamespace + "/" + ClusterName,
						AnnoTargetService: ServiceNamespace + "/" + TargetSvcName,
					},
				},
				Spec: corev1.ServiceSpec{
					Ports: []corev1.ServicePort{{Name: PortNameHTTP, Port: 80}},
				},
			}
			Expect(k8sClient.Create(ctx, hubService)).To(Succeed())
			defer func() { _ = k8sClient.Delete(ctx, hubService) }()

			_, err := reconciler.Reconcile(ctx, req)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("failed to find kubeconfig secret"))
		})

		It("Should clear Hub EndpointSlice if mode is 'pods' and remote pods vanish", func() {
			svcName := "svc-pods-vanish"
			req := reconcile.Request{NamespacedName: types.NamespacedName{Name: svcName, Namespace: ServiceNamespace}}

			// Delete the remote EndpointSlice in fake client (simulate 0 pods)
			Expect(fakeClient.DiscoveryV1().EndpointSlices(ServiceNamespace).Delete(ctx, TargetSvcName+"-slice", metav1.DeleteOptions{})).To(Succeed())

			hubService := &corev1.Service{
				ObjectMeta: metav1.ObjectMeta{
					Name:      svcName,
					Namespace: ServiceNamespace,
					Annotations: map[string]string{
						AnnoHostedCluster: ServiceNamespace + "/" + ClusterName,
						AnnoTargetService: ServiceNamespace + "/" + TargetSvcName,
						AnnoMode:          "pods",
					},
				},
				Spec: corev1.ServiceSpec{
					Ports: []corev1.ServicePort{{Name: PortNameHTTP, Port: 80}},
				},
			}
			Expect(k8sClient.Create(ctx, hubService)).To(Succeed())
			defer func() { _ = k8sClient.Delete(ctx, hubService) }()

			_, err := reconciler.Reconcile(ctx, req)
			Expect(err).NotTo(HaveOccurred())
			_, err = reconciler.Reconcile(ctx, req)
			Expect(err).NotTo(HaveOccurred())

			// EndpointSlice must exist but have empty endpoints
			slice := &discoveryv1.EndpointSlice{}
			err = k8sClient.Get(ctx, types.NamespacedName{Name: svcName + "-shadow", Namespace: ServiceNamespace}, slice)
			Expect(err).NotTo(HaveOccurred())
			Expect(slice.Endpoints).To(BeEmpty())
		})
	})
})
