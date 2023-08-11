/*
Copyright 2014 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package storage

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"reflect"
	"strconv"
	"sync"
	"unsafe"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apiserver/pkg/registry/generic"
	genericregistry "k8s.io/apiserver/pkg/registry/generic/registry"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/apiserver/pkg/storage"
	storeerr "k8s.io/apiserver/pkg/storage/errors"
	"k8s.io/apiserver/pkg/util/dryrun"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	policyclient "k8s.io/client-go/kubernetes/typed/policy/v1"
	"k8s.io/klog/v2"
	podutil "k8s.io/kubernetes/pkg/api/pod"
	api "k8s.io/kubernetes/pkg/apis/core"
	corev1 "k8s.io/kubernetes/pkg/apis/core/v1"
	"k8s.io/kubernetes/pkg/apis/core/validation"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/kubelet/client"
	"k8s.io/kubernetes/pkg/printers"
	printersinternal "k8s.io/kubernetes/pkg/printers/internalversion"
	printerstorage "k8s.io/kubernetes/pkg/printers/storage"
	registrypod "k8s.io/kubernetes/pkg/registry/core/pod"
	podrest "k8s.io/kubernetes/pkg/registry/core/pod/rest"
	"sigs.k8s.io/structured-merge-diff/v4/fieldpath"
)

// PodStorage includes storage for pods and all sub resources
type PodStorage struct {
	Pod                 *REST
	Binding             *BindingREST
	LegacyBinding       *LegacyBindingREST
	Eviction            *EvictionREST
	Status              *StatusREST
	EphemeralContainers *EphemeralContainersREST
	Log                 *podrest.LogREST
	Proxy               *podrest.ProxyREST
	Exec                *podrest.ExecREST
	Attach              *podrest.AttachREST
	PortForward         *podrest.PortForwardREST
}

// REST implements a RESTStorage for pods
type REST struct {
	*genericregistry.Store
	proxyTransport http.RoundTripper
}

// NewStorage returns a RESTStorage object that will work against pods.
func NewStorage(optsGetter generic.RESTOptionsGetter, k client.ConnectionInfoGetter, proxyTransport http.RoundTripper, podDisruptionBudgetClient policyclient.PodDisruptionBudgetsGetter) (PodStorage, error) {

	store := &genericregistry.Store{
		NewFunc:                  func() runtime.Object { return &api.Pod{} },
		NewListFunc:              func() runtime.Object { return &api.PodList{} },
		PredicateFunc:            registrypod.MatchPod,
		DefaultQualifiedResource: api.Resource("pods"),

		CreateStrategy:      registrypod.Strategy,
		UpdateStrategy:      registrypod.Strategy,
		DeleteStrategy:      registrypod.Strategy,
		ResetFieldsStrategy: registrypod.Strategy,
		ReturnDeletedObject: true,

		TableConvertor: printerstorage.TableConvertor{TableGenerator: printers.NewTableGenerator().With(printersinternal.AddHandlers)},
	}
	options := &generic.StoreOptions{
		RESTOptions: optsGetter,
		AttrFunc:    registrypod.GetAttrs,
		TriggerFunc: map[string]storage.IndexerFunc{"spec.nodeName": registrypod.NodeNameTriggerFunc},
		Indexers:    registrypod.Indexers(),
	}
	if err := store.CompleteWithOptions(options); err != nil {
		return PodStorage{}, err
	}

	statusStore := *store
	statusStore.UpdateStrategy = registrypod.StatusStrategy
	statusStore.ResetFieldsStrategy = registrypod.StatusStrategy
	ephemeralContainersStore := *store
	ephemeralContainersStore.UpdateStrategy = registrypod.EphemeralContainersStrategy

	bindingREST := &BindingREST{store: store}
	return PodStorage{
		Pod:                 &REST{store, proxyTransport},
		Binding:             &BindingREST{store: store},
		LegacyBinding:       &LegacyBindingREST{bindingREST},
		Eviction:            newEvictionStorage(store, podDisruptionBudgetClient),
		Status:              &StatusREST{store: &statusStore},
		EphemeralContainers: &EphemeralContainersREST{store: &ephemeralContainersStore},
		Log:                 &podrest.LogREST{Store: store, KubeletConn: k},
		Proxy:               &podrest.ProxyREST{Store: store, ProxyTransport: proxyTransport},
		Exec:                &podrest.ExecREST{Store: store, KubeletConn: k},
		Attach:              &podrest.AttachREST{Store: store, KubeletConn: k},
		PortForward:         &podrest.PortForwardREST{Store: store, KubeletConn: k},
	}, nil
}

// Implement ListCallbacker.
var _ = rest.ListCallbacker(&REST{})

var podListPool = sync.Pool{
	New: func() interface{} {
		return &api.PodList{
			Items: make([]api.Pod, 0, 64),
		}
	},
}

var podPool = sync.Pool{
	New: func() interface{} {
		return new(v1.Pod)
	},
}

var podZero = v1.Pod{}

func newPod() *v1.Pod {
	pod := podPool.Get().(*v1.Pod)
	prev := pod
	// reset pod
	*pod = podZero
	// pod.TypeMeta.APIVersion = "v1"
	// pod.TypeMeta.Kind = "Pod"
	// reuse previous slices and maps
	pod.ObjectMeta.Labels = prev.ObjectMeta.Labels
	pod.ObjectMeta.Annotations = prev.ObjectMeta.Annotations
	pod.ObjectMeta.OwnerReferences = prev.ObjectMeta.OwnerReferences[:0]
	pod.ObjectMeta.Finalizers = prev.ObjectMeta.Finalizers[:0]
	pod.ObjectMeta.ManagedFields = prev.ObjectMeta.ManagedFields[:0]
	pod.Spec.Volumes = prev.Spec.Volumes[:0]
	pod.Spec.InitContainers = prev.Spec.InitContainers[:0]
	pod.Spec.EphemeralContainers = prev.Spec.EphemeralContainers[:0]
	pod.Spec.NodeSelector = prev.Spec.NodeSelector
	pod.Spec.ImagePullSecrets = prev.Spec.ImagePullSecrets[:0]
	pod.Spec.Tolerations = prev.Spec.Tolerations[:0]
	pod.Spec.HostAliases = prev.Spec.HostAliases[:0]
	pod.Spec.ReadinessGates = prev.Spec.ReadinessGates[:0]
	pod.Spec.Overhead = prev.Spec.Overhead
	pod.Spec.TopologySpreadConstraints = prev.Spec.TopologySpreadConstraints[:0]
	pod.Status.Conditions = prev.Status.Conditions[:0]
	pod.Status.PodIPs = prev.Status.PodIPs[:0]
	pod.Status.InitContainerStatuses = prev.Status.InitContainerStatuses[:0]
	pod.Status.ContainerStatuses = prev.Status.ContainerStatuses[:0]
	pod.Status.EphemeralContainerStatuses = prev.Status.EphemeralContainerStatuses[:0]
	// clear map
	for k := range pod.ObjectMeta.Labels {
		delete(pod.ObjectMeta.Labels, k)
	}
	for k := range pod.ObjectMeta.Annotations {
		delete(pod.ObjectMeta.Annotations, k)
	}
	for k := range pod.Spec.NodeSelector {
		delete(pod.Spec.NodeSelector, k)
	}
	for k := range pod.Spec.Overhead {
		delete(pod.Spec.Overhead, k)
	}
	return pod
}

func (r *REST) ListCallback(ctx context.Context, options *metainternalversion.ListOptions, callback func(object runtime.Object)) error {
	// pick list from pool if it is a list all pods request
	list := podListPool.Get().(*api.PodList)
	list.Continue = ""
	list.RemainingItemCount = nil
	list.Items = list.Items[:0]
	defer podListPool.Put(list)

	klog.V(3).Infof("podstore: ListCallback pick a pod list from pool, len=%d cap=%d", len(list.Items), cap(list.Items))

	pods := []*v1.Pod{}
	defer func() {
		for _, pod := range pods {
			podPool.Put(pod)
		}
	}()

	appendListItem := func(v reflect.Value, data []byte, rev uint64, pred storage.SelectionPredicate, codec runtime.Codec, versioner storage.Versioner, newItemFunc func() runtime.Object) error {
		// proto unmarshal
		unk := runtime.Unknown{}
		if err := unk.Unmarshal(data); err != nil {
			return err
		}

		// new a cleaned v1.Pod
		pod := newPod()
		if err := pod.Unmarshal(unk.Raw); err != nil {
			podPool.Put(pod)
			return err
		}

		// return pool immediately if not matched, otherwise put it to outer []*v1.Pod then return pool later
		if matched, err := pred.Matches(pod); err != nil || !matched {
			podPool.Put(pod)
			return nil
		} else {
			pods = append(pods, pod)
		}

		// reuse or create a *core.Pod then convert *v1.Pod to it
		if len(list.Items) < cap(list.Items) {
			hdr := (*reflect.SliceHeader)(unsafe.Pointer(&list.Items))
			hdr.Len++
			klog.V(4).Infof("podstore: ListCallback reuse an existing pod slot, hdr.Len=%d, cap=%d", hdr.Len, cap(list.Items))
		} else {
			list.Items = append(list.Items, api.Pod{})
			klog.V(4).Infof("podstore: ListCallback create a new pod to pod list, len=%d, cap=%d", len(list.Items), cap(list.Items))
		}
		item := &list.Items[len(list.Items)-1]
		if err := corev1.Convert_v1_Pod_To_core_Pod(pod, item, nil); err != nil {
			return err
		}
		item.ObjectMeta.ResourceVersion = strconv.FormatUint(rev, 10)

		klog.V(4).Infof("podstore: ListCallback append a matched pod to pod list, name=%s len=%d, cap=%d", item.Name, len(list.Items), cap(list.Items))

		return nil
	}

	err := r.Store.GetList(ctx, options, list, appendListItem)
	if err != nil {
		return err
	}

	klog.V(3).Infof("podstore: ListCallback get a pod list result, len=%d cap=%d", len(list.Items), cap(list.Items))

	callback(list)
	return nil
}

// Implement Redirector.
var _ = rest.Redirector(&REST{})

// ResourceLocation returns a pods location from its HostIP
func (r *REST) ResourceLocation(ctx context.Context, name string) (*url.URL, http.RoundTripper, error) {
	return registrypod.ResourceLocation(ctx, r, r.proxyTransport, name)
}

// Implement ShortNamesProvider
var _ rest.ShortNamesProvider = &REST{}

// ShortNames implements the ShortNamesProvider interface. Returns a list of short names for a resource.
func (r *REST) ShortNames() []string {
	return []string{"po"}
}

// Implement CategoriesProvider
var _ rest.CategoriesProvider = &REST{}

// Categories implements the CategoriesProvider interface. Returns a list of categories a resource is part of.
func (r *REST) Categories() []string {
	return []string{"all"}
}

// BindingREST implements the REST endpoint for binding pods to nodes when etcd is in use.
type BindingREST struct {
	store *genericregistry.Store
}

// NamespaceScoped fulfill rest.Scoper
func (r *BindingREST) NamespaceScoped() bool {
	return r.store.NamespaceScoped()
}

// New creates a new binding resource
func (r *BindingREST) New() runtime.Object {
	return &api.Binding{}
}

var _ = rest.NamedCreater(&BindingREST{})

// Create ensures a pod is bound to a specific host.
func (r *BindingREST) Create(ctx context.Context, name string, obj runtime.Object, createValidation rest.ValidateObjectFunc, options *metav1.CreateOptions) (out runtime.Object, err error) {
	binding, ok := obj.(*api.Binding)
	if !ok {
		return nil, errors.NewBadRequest(fmt.Sprintf("not a Binding object: %#v", obj))
	}

	if name != binding.Name {
		return nil, errors.NewBadRequest("name in URL does not match name in Binding object")
	}

	// TODO: move me to a binding strategy
	if errs := validation.ValidatePodBinding(binding); len(errs) != 0 {
		return nil, errs.ToAggregate()
	}

	if createValidation != nil {
		if err := createValidation(ctx, binding.DeepCopyObject()); err != nil {
			return nil, err
		}
	}

	err = r.assignPod(ctx, binding.UID, binding.ResourceVersion, binding.Name, binding.Target.Name, binding.Annotations, dryrun.IsDryRun(options.DryRun))
	out = &metav1.Status{Status: metav1.StatusSuccess}
	return
}

// setPodHostAndAnnotations sets the given pod's host to 'machine' if and only if
// the pod is unassigned and merges the provided annotations with those of the pod.
// Returns the current state of the pod, or an error.
func (r *BindingREST) setPodHostAndAnnotations(ctx context.Context, podUID types.UID, podResourceVersion, podID, machine string, annotations map[string]string, dryRun bool) (finalPod *api.Pod, err error) {
	podKey, err := r.store.KeyFunc(ctx, podID)
	if err != nil {
		return nil, err
	}

	var preconditions *storage.Preconditions
	if podUID != "" || podResourceVersion != "" {
		preconditions = &storage.Preconditions{}
		if podUID != "" {
			preconditions.UID = &podUID
		}
		if podResourceVersion != "" {
			preconditions.ResourceVersion = &podResourceVersion
		}
	}

	err = r.store.Storage.GuaranteedUpdate(ctx, podKey, &api.Pod{}, false, preconditions, storage.SimpleUpdate(func(obj runtime.Object) (runtime.Object, error) {
		pod, ok := obj.(*api.Pod)
		if !ok {
			return nil, fmt.Errorf("unexpected object: %#v", obj)
		}
		if pod.DeletionTimestamp != nil {
			return nil, fmt.Errorf("pod %s is being deleted, cannot be assigned to a host", pod.Name)
		}
		if pod.Spec.NodeName != "" {
			return nil, fmt.Errorf("pod %v is already assigned to node %q", pod.Name, pod.Spec.NodeName)
		}
		pod.Spec.NodeName = machine
		if pod.Annotations == nil {
			pod.Annotations = make(map[string]string)
		}
		for k, v := range annotations {
			pod.Annotations[k] = v
		}
		podutil.UpdatePodCondition(&pod.Status, &api.PodCondition{
			Type:   api.PodScheduled,
			Status: api.ConditionTrue,
		})
		finalPod = pod
		return pod, nil
	}), dryRun, nil)
	return finalPod, err
}

// assignPod assigns the given pod to the given machine.
func (r *BindingREST) assignPod(ctx context.Context, podUID types.UID, podResourceVersion, podID string, machine string, annotations map[string]string, dryRun bool) (err error) {
	if _, err = r.setPodHostAndAnnotations(ctx, podUID, podResourceVersion, podID, machine, annotations, dryRun); err != nil {
		err = storeerr.InterpretGetError(err, api.Resource("pods"), podID)
		err = storeerr.InterpretUpdateError(err, api.Resource("pods"), podID)
		if _, ok := err.(*errors.StatusError); !ok {
			err = errors.NewConflict(api.Resource("pods/binding"), podID, err)
		}
	}
	return
}

var _ = rest.Creater(&LegacyBindingREST{})

// LegacyBindingREST implements the REST endpoint for binding pods to nodes when etcd is in use.
type LegacyBindingREST struct {
	bindingRest *BindingREST
}

// NamespaceScoped fulfill rest.Scoper
func (r *LegacyBindingREST) NamespaceScoped() bool {
	return r.bindingRest.NamespaceScoped()
}

// New creates a new binding resource
func (r *LegacyBindingREST) New() runtime.Object {
	return r.bindingRest.New()
}

// Create ensures a pod is bound to a specific host.
func (r *LegacyBindingREST) Create(ctx context.Context, obj runtime.Object, createValidation rest.ValidateObjectFunc, options *metav1.CreateOptions) (out runtime.Object, err error) {
	metadata, err := meta.Accessor(obj)
	if err != nil {
		return nil, errors.NewBadRequest(fmt.Sprintf("not a Binding object: %T", obj))
	}
	return r.bindingRest.Create(ctx, metadata.GetName(), obj, createValidation, options)
}

// StatusREST implements the REST endpoint for changing the status of a pod.
type StatusREST struct {
	store *genericregistry.Store
}

// New creates a new pod resource
func (r *StatusREST) New() runtime.Object {
	return &api.Pod{}
}

// Get retrieves the object from the storage. It is required to support Patch.
func (r *StatusREST) Get(ctx context.Context, name string, options *metav1.GetOptions) (runtime.Object, error) {
	return r.store.Get(ctx, name, options)
}

// Update alters the status subset of an object.
func (r *StatusREST) Update(ctx context.Context, name string, objInfo rest.UpdatedObjectInfo, createValidation rest.ValidateObjectFunc, updateValidation rest.ValidateObjectUpdateFunc, forceAllowCreate bool, options *metav1.UpdateOptions) (runtime.Object, bool, error) {
	// We are explicitly setting forceAllowCreate to false in the call to the underlying storage because
	// subresources should never allow create on update.
	return r.store.Update(ctx, name, objInfo, createValidation, updateValidation, false, options)
}

// GetResetFields implements rest.ResetFieldsStrategy
func (r *StatusREST) GetResetFields() map[fieldpath.APIVersion]*fieldpath.Set {
	return r.store.GetResetFields()
}

func (r *StatusREST) ConvertToTable(ctx context.Context, object runtime.Object, tableOptions runtime.Object) (*metav1.Table, error) {
	return r.store.ConvertToTable(ctx, object, tableOptions)
}

// EphemeralContainersREST implements the REST endpoint for adding EphemeralContainers
type EphemeralContainersREST struct {
	store *genericregistry.Store
}

var _ = rest.Patcher(&EphemeralContainersREST{})

// Get retrieves the object from the storage. It is required to support Patch.
func (r *EphemeralContainersREST) Get(ctx context.Context, name string, options *metav1.GetOptions) (runtime.Object, error) {
	if !utilfeature.DefaultFeatureGate.Enabled(features.EphemeralContainers) {
		return nil, errors.NewBadRequest("feature EphemeralContainers disabled")
	}

	return r.store.Get(ctx, name, options)
}

// New creates a new pod resource
func (r *EphemeralContainersREST) New() runtime.Object {
	return &api.Pod{}
}

// Update alters the EphemeralContainers field in PodSpec
func (r *EphemeralContainersREST) Update(ctx context.Context, name string, objInfo rest.UpdatedObjectInfo, createValidation rest.ValidateObjectFunc, updateValidation rest.ValidateObjectUpdateFunc, forceAllowCreate bool, options *metav1.UpdateOptions) (runtime.Object, bool, error) {
	if !utilfeature.DefaultFeatureGate.Enabled(features.EphemeralContainers) {
		return nil, false, errors.NewBadRequest("feature EphemeralContainers disabled")
	}

	// We are explicitly setting forceAllowCreate to false in the call to the underlying storage because
	// subresources should never allow create on update.
	return r.store.Update(ctx, name, objInfo, createValidation, updateValidation, false, options)
}
