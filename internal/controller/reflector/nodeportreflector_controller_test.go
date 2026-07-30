package reflector

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	reflectorv1alpha1 "github.com/bizcochillo/hcp-nodeport-reflector/api/reflector/v1alpha1"
)

var _ = Describe("NodePortReflector Controller", func() {
	const (
		Namespace     = "default"
		ClusterName   = "mock-cluster"
		TargetSvcName = "remote-app"
	)

	var (
		reconciler   *NodePortReflectorReconciler
		fakeClient   *fake.Clientset
		secretObject *corev1.Secret
	)

	BeforeEach(func() {
		// 1. Setup Fake Remote Cluster con 2 Worker Nodes y 1 Master
		fakeClient = fake.NewSimpleClientset(
			&corev1.Node{
				ObjectMeta: metav1.ObjectMeta{Name: "worker-1"},
				Status:     corev1.NodeStatus{Addresses: []corev1.NodeAddress{{Type: corev1.NodeInternalIP, Address: "10.0.0.1"}}},
			},
			&corev1.Node{
				ObjectMeta: metav1.ObjectMeta{Name: "worker-2"},
				Status:     corev1.NodeStatus{Addresses: []corev1.NodeAddress{{Type: corev1.NodeInternalIP, Address: "10.0.0.2"}}},
			},
			&corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:   "master-1",
					Labels: map[string]string{"node-role.kubernetes.io/control-plane": ""},
				},
				Status: corev1.NodeStatus{Addresses: []corev1.NodeAddress{{Type: corev1.NodeInternalIP, Address: "10.0.0.99"}}},
			},
			// Remote Target Service (MULTIPLES PUERTOS)
			&corev1.Service{
				ObjectMeta: metav1.ObjectMeta{Name: TargetSvcName, Namespace: Namespace},
				Spec: corev1.ServiceSpec{
					Type:                  corev1.ServiceTypeNodePort,
					ExternalTrafficPolicy: corev1.ServiceExternalTrafficPolicyCluster, // Por defecto "Cluster" -> Mode: nodes
					Ports: []corev1.ServicePort{
						{Name: "http", Port: 80, NodePort: 30080, Protocol: corev1.ProtocolTCP},
						{Name: "https", Port: 443, NodePort: 30443, Protocol: corev1.ProtocolTCP},
					},
				},
			},
			// Remote EndpointSlice (Pod ready solo en worker-1)
			&discoveryv1.EndpointSlice{
				ObjectMeta: metav1.ObjectMeta{
					Name:      TargetSvcName + "-slice",
					Namespace: Namespace,
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
		reconciler = &NodePortReflectorReconciler{
			Client: k8sClient,
			Scheme: k8sClient.Scheme(),
			RemoteClientBuilder: func(kubeconfigBytes []byte) (kubernetes.Interface, error) {
				return fakeClient, nil
			},
		}

		// 3. Create Required Secret in Hub
		secretObject = &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: ClusterName + "-admin-kubeconfig", Namespace: Namespace},
			Data:       map[string][]byte{"kubeconfig": []byte("dummy-kubeconfig-data")},
		}
		_ = k8sClient.Create(ctx, secretObject)
	})

	AfterEach(func() {
		_ = k8sClient.Delete(ctx, secretObject)
	})

	It("Should sync multiple ports and use 'nodes' mode when ExternalTrafficPolicy is Cluster", func() {
		crName := "npr-multiport-cluster"
		req := reconcile.Request{NamespacedName: types.NamespacedName{Name: crName, Namespace: Namespace}}

		npr := &reflectorv1alpha1.NodePortReflector{
			ObjectMeta: metav1.ObjectMeta{Name: crName, Namespace: Namespace},
			Spec: reflectorv1alpha1.NodePortReflectorSpec{
				HostedCluster: reflectorv1alpha1.NamespacedRef{Name: ClusterName, Namespace: Namespace},
				TargetService: reflectorv1alpha1.NamespacedRef{Name: TargetSvcName, Namespace: Namespace},
			},
		}
		Expect(k8sClient.Create(ctx, npr)).To(Succeed())
		defer func() { _ = k8sClient.Delete(ctx, npr) }()

		// Reconcile
		_, err := reconciler.Reconcile(ctx, req)
		Expect(err).NotTo(HaveOccurred())

		// Verificar que el Service del Hub tiene TODOS los puertos mapeados
		hubSvc := &corev1.Service{}
		err = k8sClient.Get(ctx, req.NamespacedName, hubSvc)
		Expect(err).NotTo(HaveOccurred())
		Expect(hubSvc.Spec.Ports).To(HaveLen(2))
		Expect(hubSvc.Spec.Ports[0].TargetPort.IntVal).To(Equal(int32(30080)))

		// Verificar que el EndpointSlice contiene AMBOS worker nodes (porque es policy: Cluster)
		slice := &discoveryv1.EndpointSlice{}
		err = k8sClient.Get(ctx, types.NamespacedName{Name: crName + "-shadow", Namespace: Namespace}, slice)
		Expect(err).NotTo(HaveOccurred())
		Expect(slice.Endpoints).To(HaveLen(1))
		Expect(slice.Endpoints[0].Addresses).To(ConsistOf("10.0.0.1", "10.0.0.2"))
		Expect(slice.Ports).To(HaveLen(2))
	})

	It("Should use 'pods' mode when ExternalTrafficPolicy is Local", func() {
		// Modificamos el servicio remoto en el cliente fake a "Local"
		remoteSvc, _ := fakeClient.CoreV1().Services(Namespace).Get(ctx, TargetSvcName, metav1.GetOptions{})
		remoteSvc.Spec.ExternalTrafficPolicy = corev1.ServiceExternalTrafficPolicyLocal
		_, _ = fakeClient.CoreV1().Services(Namespace).Update(ctx, remoteSvc, metav1.UpdateOptions{})

		crName := "npr-policy-local"
		req := reconcile.Request{NamespacedName: types.NamespacedName{Name: crName, Namespace: Namespace}}

		npr := &reflectorv1alpha1.NodePortReflector{
			ObjectMeta: metav1.ObjectMeta{Name: crName, Namespace: Namespace},
			Spec: reflectorv1alpha1.NodePortReflectorSpec{
				HostedCluster: reflectorv1alpha1.NamespacedRef{Name: ClusterName, Namespace: Namespace},
				TargetService: reflectorv1alpha1.NamespacedRef{Name: TargetSvcName, Namespace: Namespace},
			},
		}
		Expect(k8sClient.Create(ctx, npr)).To(Succeed())
		defer func() { _ = k8sClient.Delete(ctx, npr) }()

		_, err := reconciler.Reconcile(ctx, req)
		Expect(err).NotTo(HaveOccurred())

		// Verificar que el EndpointSlice SOLO contiene el worker-1 (porque es policy: Local)
		slice := &discoveryv1.EndpointSlice{}
		err = k8sClient.Get(ctx, types.NamespacedName{Name: crName + "-shadow", Namespace: Namespace}, slice)
		Expect(err).NotTo(HaveOccurred())
		Expect(slice.Endpoints[0].Addresses).To(Equal([]string{"10.0.0.1"}))
	})

	It("Should clear Hub endpoints if the remote service goes missing", func() {
		crName := "npr-remote-missing"
		req := reconcile.Request{NamespacedName: types.NamespacedName{Name: crName, Namespace: Namespace}}

		npr := &reflectorv1alpha1.NodePortReflector{
			ObjectMeta: metav1.ObjectMeta{Name: crName, Namespace: Namespace},
			Spec: reflectorv1alpha1.NodePortReflectorSpec{
				HostedCluster: reflectorv1alpha1.NamespacedRef{Name: ClusterName, Namespace: Namespace},
				TargetService: reflectorv1alpha1.NamespacedRef{Name: TargetSvcName, Namespace: Namespace},
			},
		}
		Expect(k8sClient.Create(ctx, npr)).To(Succeed())
		defer func() { _ = k8sClient.Delete(ctx, npr) }()

		// Reconciliar estado inicial
		_, _ = reconciler.Reconcile(ctx, req)

		// Borramos el servicio remoto
		_ = fakeClient.CoreV1().Services(Namespace).Delete(ctx, TargetSvcName, metav1.DeleteOptions{})

		// Reconciliar otra vez
		_, err := reconciler.Reconcile(ctx, req)
		Expect(err).NotTo(HaveOccurred())

		// El EndpointSlice debe estar vacío
		slice := &discoveryv1.EndpointSlice{}
		err = k8sClient.Get(ctx, types.NamespacedName{Name: crName + "-shadow", Namespace: Namespace}, slice)
		Expect(err).NotTo(HaveOccurred())
		Expect(slice.Endpoints).To(BeEmpty())
	})
})
