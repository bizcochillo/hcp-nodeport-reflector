package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// NamespacedRef defines a reference to a resource in a specific namespace.
type NamespacedRef struct {
	Name string `json:"name"`
	// +optional
	Namespace string `json:"namespace,omitempty"`
}

// NodePortReflectorSpec defines the desired state of NodePortReflector.
type NodePortReflectorSpec struct {
	// HostedCluster points to the remote cluster (cluster name and secret namespace).
	HostedCluster NamespacedRef `json:"hostedCluster"`

	// TargetService points to the NodePort Service in the Hosted cluster.
	TargetService NamespacedRef `json:"targetService"`

	// +kubebuilder:validation:Optional
	// +kubebuilder:default=false
	// Headless determines if the created shadow service should be Headless (ClusterIP: None).
	Headless bool `json:"headless,omitempty"`
}

// PortStatus records which ports are being synchronized in the Hub.
type PortStatus struct {
	Name     string `json:"name,omitempty"`
	Port     int32  `json:"port"`
	NodePort int32  `json:"nodePort"`
	Protocol string `json:"protocol"`
}

// NodePortReflectorStatus defines the observed state of NodePortReflector.
type NodePortReflectorStatus struct {
	Phase          string             `json:"phase,omitempty"`
	DetectedPolicy string             `json:"detectedPolicy,omitempty"`
	ActiveNodes    int                `json:"activeNodes"`
	SyncedPorts    []PortStatus       `json:"syncedPorts,omitempty"`
	Conditions     []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Policy",type=string,JSONPath=`.status.detectedPolicy`
// +kubebuilder:printcolumn:name="Active Nodes",type=integer,JSONPath=`.status.activeNodes`
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"

// NodePortReflector is the Schema for the nodeportreflectors API
type NodePortReflector struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   NodePortReflectorSpec   `json:"spec,omitempty"`
	Status NodePortReflectorStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// NodePortReflectorList contains a list of NodePortReflector
type NodePortReflectorList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []NodePortReflector `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(scheme *runtime.Scheme) error {
		scheme.AddKnownTypes(GroupVersion,
			&NodePortReflector{},
			&NodePortReflectorList{},
		)
		return nil
	})
}
