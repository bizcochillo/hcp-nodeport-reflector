package controller

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

var _ = Describe("Service Controller - Full Offline Sync", func() {
	const (
		ServiceName      = "test-shadow-svc"
		ServiceNamespace = "default"
		TargetCluster    = "mock-cluster"
		TargetNamespace  = "mock-namespace"
		TargetSvcName    = "remote-app"
		SecretName       = "mock-cluster-admin-kubeconfig"
		ExpectedNodeIP   = "10.0.0.5"
	)

	Context("When reconciling with a mocked Hosted Cluster", func() {
		It("Should generate the EndpointSlice in the Hub", func() {
			ctx := context.Background()

			// 1. Create the target namespace for the secret
			ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: TargetNamespace}}
			Expect(k8sClient.Create(ctx, ns)).Should(Succeed())

			// 2. Create the Dummy Hub Resources (Service + Secret)
			hubService := &corev1.Service{
				ObjectMeta: metav1.ObjectMeta{
					Name:      ServiceName,
					Namespace: ServiceNamespace,
					Annotations: map[string]string{
						AnnoHostedCluster: TargetNamespace + "/" + TargetCluster,
						AnnoTargetService: "default/" + TargetSvcName,
						AnnoMode:          "nodes",
					},
				},
				Spec: corev1.ServiceSpec{
					ClusterIP: "None",
					Ports:     []corev1.ServicePort{{Name: "http", Port: 80}},
				},
			}
			Expect(k8sClient.Create(ctx, hubService)).Should(Succeed())

			kubeconfigSecret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: SecretName, Namespace: TargetNamespace},
				Data:       map[string][]byte{"kubeconfig": []byte("dummy-data")},
			}
			Expect(k8sClient.Create(ctx, kubeconfigSecret)).Should(Succeed())

			// 3. Set up the Fake Hosted Cluster
			remoteNode := &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{Name: "worker-1"},
				Status: corev1.NodeStatus{
					Addresses: []corev1.NodeAddress{{Type: corev1.NodeInternalIP, Address: ExpectedNodeIP}},
				},
			}

			remoteService := &corev1.Service{
				ObjectMeta: metav1.ObjectMeta{Name: TargetSvcName, Namespace: "default"},
				Spec: corev1.ServiceSpec{
					Ports: []corev1.ServicePort{{NodePort: 32222}},
				},
			}

			remoteEndpointSlice := &discoveryv1.EndpointSlice{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "remote-app-slice",
					Namespace: "default",
					Labels:    map[string]string{"kubernetes.io/service-name": TargetSvcName},
				},
				Endpoints: []discoveryv1.Endpoint{
					{
						Addresses: []string{"10.244.0.5"}, // Pod IP (ignored in node mode)
						NodeName:  ptr.To("worker-1"),     // The active node
						Conditions: discoveryv1.EndpointConditions{
							Ready: ptr.To(true),
						},
					},
				},
			}

			// Pre-load the fake client with our remote objects
			fakeHostedClient := fake.NewSimpleClientset(remoteNode, remoteService, remoteEndpointSlice)

			// 4. Configure the Reconciler with Dependency Injection
			reconciler := &ServiceReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
				// ---> ADD THESE TWO LINES <---
				Connections:    make(map[string]context.CancelFunc),
				TriggerChannel: make(chan event.GenericEvent, 1024),
				// -----------------------------
				RemoteClientBuilder: func(kubeconfigBytes []byte) (kubernetes.Interface, error) {
					return fakeHostedClient, nil // Always return our fake client
				},
			}

			// 5. Trigger the Reconcile Loop
			// 5. Trigger the Reconcile Loop
			req := reconcile.Request{NamespacedName: types.NamespacedName{Name: ServiceName, Namespace: ServiceNamespace}}

			// PRIMERA PASADA: El controlador detecta que el NodePort cambió y actualiza el Hub Service.
			// Luego sale inmediatamente esperando que Kubernetes vuelva a encolar el evento.
			_, err := reconciler.Reconcile(ctx, req)
			Expect(err).NotTo(HaveOccurred())

			// SEGUNDA PASADA: Simulamos que Kubernetes vuelve a lanzar el evento.
			// Ahora el puerto está sincronizado, así que pasará a crear el EndpointSlice.
			_, err = reconciler.Reconcile(ctx, req)
			Expect(err).NotTo(HaveOccurred())

			// 6. Verify the Hub EndpointSlice was created correctly
			// 6. Verify the Hub EndpointSlice was created correctly
			createdSlice := &discoveryv1.EndpointSlice{}
			sliceKey := types.NamespacedName{Name: ServiceName + "-shadow", Namespace: ServiceNamespace}

			Expect(k8sClient.Get(ctx, sliceKey, createdSlice)).Should(Succeed())
			Expect(createdSlice.Endpoints).To(HaveLen(1))
			Expect(createdSlice.Endpoints[0].Addresses).To(ContainElement(ExpectedNodeIP))
			Expect(createdSlice.Ports[0].Port).To(Equal(ptr.To[int32](32222)))

			// Cleanup
			Expect(k8sClient.Delete(ctx, hubService)).Should(Succeed())
			Expect(k8sClient.Delete(ctx, kubeconfigSecret)).Should(Succeed())
			Expect(k8sClient.Delete(ctx, ns)).Should(Succeed())
		})
	})
})
