package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// NamespacedRef define una referencia a un recurso en un namespace específico
type NamespacedRef struct {
	Name string `json:"name"`
	// +optional
	Namespace string `json:"namespace,omitempty"`
}

// NodePortReflectorSpec define el estado deseado
type NodePortReflectorSpec struct {
	// HostedCluster apunta al clúster remoto (nombre del clúster y namespace del secret)
	HostedCluster NamespacedRef `json:"hostedCluster"`

	// TargetService apunta al NodePort Service en el clúster Hosted
	TargetService NamespacedRef `json:"targetService"`
}

// PortStatus registra qué puertos se están sincronizando en el Hub
type PortStatus struct {
	Name     string `json:"name,omitempty"`
	Port     int32  `json:"port"`
	NodePort int32  `json:"nodePort"`
	Protocol string `json:"protocol"`
}

// NodePortReflectorStatus define el estado observado del reflector
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
