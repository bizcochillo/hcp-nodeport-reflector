# hcp-nodeport-reflector
A Kubernetes operator designed to seamlessly bridge network traffic from an OpenShift HyperShift (HCP) Management Cluster (Hub) directly to workloads running inside Hosted Clusters.

In a HyperShift architecture, worker nodes and workloads live in a Hosted Cluster, while the control plane lives in the Management (Hub) Cluster. Exposing Hosted Cluster applications usually requires provisioning external cloud load balancers. But what if you want to route traffic centrally through the Hub cluster's Ingress Controller or Gateway?

The **HCP NodePort Reflector** solves this by creating a direct L4 network bridge. 

When you deploy a `NodePortReflector` Custom Resource in the Hub cluster, the operator dynamically connects to the Hosted Cluster, inspects a target `NodePort` service, and automatically provisions a "Shadow Service" and an `EndpointSlice` in the Hub. This allows resources in the Hub (like a central HAProxy Router) to send traffic directly to the worker nodes of the Hosted Cluster.

**Key Features:**
*   **Zero-SNAT IP Preservation:** It intelligently reads the remote service's `externalTrafficPolicy`. If set to `Local`, the operator maps endpoints exclusively to the nodes actively running the pods, preserving the original client IP across cluster boundaries.
*   **Multi-Port Sync:** Automatically discovers and reflects all exposed ports from the remote service.
*   **Dynamic Synchronization:** Continuously monitors the Hosted Cluster to update the Hub's endpoints if worker nodes scale up/down or if the remote service changes.
*   **Headless Service Support:** Optionally configure the shadow service as Headless (`ClusterIP: None`) to bypass the Hub's kube-proxy and allow DNS to return the raw target node IPs directly.
*   **Ingress Chaining:** Enables advanced multi-tier routing (e.g., exposing a Hosted Cluster's Ingress controller through the Hub's Ingress controller using Passthrough routes).

---

## Architecture & How it Works

1. The Operator runs in the **Hub Cluster**.
2. A user creates a `NodePortReflector` Custom Resource in the Hub.
3. The controller locates the automatically generated `kubeconfig` secret for the specified Hosted Cluster.
4. It connects to the Hosted Cluster and reads the target `NodePort` Service.
5. It creates a **Shadow Service** (type `ClusterIP`) and a corresponding **EndpointSlice** in the Hub, mapping the Hub service directly to the IPs of the Hosted Cluster's worker nodes.

---

## Usage Example

To reflect a service, you need to expose your application as a `NodePort` in the Hosted Cluster, and then create a `NodePortReflector` in the Hub Cluster.

### 1. In your Hosted Cluster
Deploy your application and expose it using a `NodePort` service. To preserve the client's original IP, use `externalTrafficPolicy: Local`.

```yaml
apiVersion: v1
kind: Service
metadata:
  name: my-backend-app
  namespace: default
spec:
  type: NodePort
  externalTrafficPolicy: Local
  selector:
    app: my-app
  ports:
  - name: http
    port: 80
    targetPort: 8080
```

### 2. In your Hub Cluster
Create the Custom Resource. The operator will automatically find the Hosted Cluster, locate `my-backend-app`, and create a shadow service in the Hub.

```yaml
apiVersion: reflector.bizcochillo.io/v1alpha1
kind: NodePortReflector
metadata:
  name: reflect-my-backend
  namespace: default
spec:
  # headless: true               # Optional: Set to true to create a Headless Service (ClusterIP: None)
  hostedCluster:
    name: my-hosted-cluster-name # The name of the HyperShift HostedCluster
    namespace: clusters          # The namespace where the HostedCluster resides in the Hub
  targetService:
    name: my-backend-app         # The name of the Service inside the Hosted Cluster
    namespace: default           # The namespace of the Service inside the Hosted Cluster
```
**Note on Headless Services**: If your architecture requires client applications to handle their own load balancing or directly address the underlying node IPs via DNS, you can set `headless: true`. This instructs the operator to provision the shadow service with `ClusterIP: None`.

Once applied, you can verify the status:

```bash
[user@host ~]$ oc get nodeportreflector reflect-my-backend
NAME                PHASE    POLICY    ACTIVE NODES   AGE
reflect-my-backend  Synced   Cluster   2              89m
```
You can now point any Hub Ingress/Route to the newly created shadow service (`reflect-my-backend`).

### 3. Advanced: Ingress Chaining (Edge-to-Cluster Proxying)
A powerful architectural pattern enabled by this operator is chaining Ingress controllers. You can route wildcard traffic from the Hub directly to an Ingress Controller running inside the Hosted Cluster.

By leveraging `externalTrafficPolicy: Local` on the remote Ingress Service, the client's original IP is preserved across clusters without SNAT. Furthermore, by using an OpenShift `passthrough` Route in the Hub, the Hub router forwards the raw TCP/TLS stream based on SNI (Server Name Indication).

This means: 
- The `Host` header is never rewritten.
- The Hosted Cluster handles TLS termination natively with its own certificates.

**In the Hosted cluster**
We create a service `router-nodeport` of NodePort type which will be monitored by the reflector to create the EndpointSlices. 

```yaml
kind: Service
apiVersion: v1
metadata:
  name: router-nodeport
  namespace: openshift-ingress
spec:
  externalTrafficPolicy: Cluster
  ports:
    - name: http
      protocol: TCP
      port: 80
      targetPort: 80
    - name: https
      protocol: TCP
      port: 443
      targetPort: 443
  internalTrafficPolicy: Cluster
  type: NodePort
  selector:
    ingresscontroller.operator.openshift.io/deployment-ingresscontroller: default
```

**In the Hub cluster**

We enable wildcard routes in the targeted Ingress controller of the Hub cluster 
```bash
oc patch ingresscontroller default -n openshift-ingress-operator \
  --type=merge \
  -p '{"spec": {"routeAdmission": {"wildcardPolicy": "WildcardsAllowed"}}}'
```

We create a NodePortReflector, that is monitoring the just created `NodePort` service named `router-nodeport` in the cluster `my-hosted-cluster-name`
```yaml
apiVersion: reflector.bizcochillo.io/v1alpha1
kind: NodePortReflector
metadata:
  name: router-hosted
  namespace: demo-reflectors
spec:
  hostedCluster:
    name: my-hosted-cluster-name # The name of the HyperShift HostedCluster
    namespace: clusters          # The namespace where the HostedCluster resides in the Hub
  targetService:
    name: router-nodeport
    namespace: openshift-ingress
```

We create routes for HTTP and HTTPS 
```yaml
kind: Route
apiVersion: route.openshift.io/v1
metadata:
  name: router-hosted-http
  namespace: demo-reflectors
spec:
  host: wildcard.apps2.hosted.hypershift.lab
  path: /
  to:
    kind: Service
    name: router-hosted
    weight: 100
  port:
    targetPort: http
  wildcardPolicy: Subdomain
---
kind: Route
apiVersion: route.openshift.io/v1
metadata:
  name: router-hosted-https
  namespace: demo-reflectors
spec:
  host: wildcard.apps2.hosted.hypershift.lab
  to:
    kind: Service
    name: router-hosted
    weight: 100
  port:
    targetPort: https
  tls:
    termination: passthrough
    insecureEdgeTerminationPolicy: None
  wildcardPolicy: Subdomain
```

To use this approach, you must configure a wildcard DNS record (e.g., `*.apps2.hosted.hypershift.lab`) that resolves to the Management (Hub) cluster. Once configured, all HTTP and HTTPS traffic targeting this wildcard domain is received by the Hub's Ingress Controller and transparently forwarded to the default Ingress Controller running inside the Hosted cluster.

## Installation 
The easiest way to install the operator in your Management (Hub) cluster is by applying the pre-built distribution manifest. This YAML bundle contains the CRDs, RBAC rules, and the Operator Deployment.

```bash
oc apply -f [https://raw.githubusercontent.com/bizcochillo/hcp-nodeport-reflector/v0.2.0/dist/reflector-install-v0.2.0.yaml](https://raw.githubusercontent.com/bizcochillo/hcp-nodeport-reflector/v0.2.0/dist/reflector-install-v0.2.0.yaml)
```

Once installed, verify that the controller pod is running:
```bash
oc get pods -n hcp-nodeport-reflector-system
```


## Developer Guide

### Prerequisites
- go version v1.24.6+
- docker version 17.03+.
- kubectl version v1.11.3+.
- Access to a Kubernetes v1.11.3+ cluster.

### Local Build and Deployment
**1. Build and push your image to your registry (specified by `IMG`):**

```sh
make docker-build docker-push IMG=<some-registry>/hcp-nodeport-reflector:tag
```

**2. Deploy the Manager to the cluster:**

**NOTE:** This image ought to be published in the personal registry you specified.
And it is required to have access to pull the image from the working environment.
Make sure you have the proper permission to the registry if the above commands don’t work.

**Install the CRDs into the cluster:**
```bash
make deploy IMG=<some-registry>/hcp-nodeport-reflector:tag
```

**3. Test it out with samples:**
//TODO
```sh
oc apply -k config/samples/
```

**Generating a New Release Bundle:**
If you make changes and want to generate a new standalone `install-*.yaml` for distribution:
```sh
make build-installer IMG=<some-registry>/hcp-nodeport-reflector:tag
```
This generates a file in the `dist/` directory containing all resources built with Kustomize.

**Cleanup**

```sh
oc delete -k config/samples/
make undeploy
```
## Contributing
Run `make help` for more information on all potential `make` targets

More information can be found via the [Kubebuilder Documentation](https://book.kubebuilder.io/introduction.html)

## License

Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

