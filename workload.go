/*
Copyright The Kubernetes Authors.

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

package main

import (
	"context"
	"fmt"
	"sort"

	autoscalingv1 "k8s.io/api/autoscaling/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

const helmReleaseAnnotation = "meta.helm.sh/release-name"

// workload is the part of a controller the page needs: the pod template with the
// manifest's requests and limits (what Helm rendered), and how to find its pods.
type workload struct {
	containers []corev1.Container
	selector   *metav1.LabelSelector
	helm       string
}

func getWorkload(ctx context.Context, kube kubernetes.Interface, namespace string, ref *autoscalingv1.CrossVersionObjectReference) (*workload, error) {
	if ref == nil {
		return nil, fmt.Errorf("vpa has no targetRef")
	}
	get := metav1.GetOptions{}
	switch ref.Kind {
	case "Deployment":
		o, err := kube.AppsV1().Deployments(namespace).Get(ctx, ref.Name, get)
		if err != nil {
			return nil, err
		}
		return &workload{o.Spec.Template.Spec.Containers, o.Spec.Selector, o.Annotations[helmReleaseAnnotation]}, nil
	case "StatefulSet":
		o, err := kube.AppsV1().StatefulSets(namespace).Get(ctx, ref.Name, get)
		if err != nil {
			return nil, err
		}
		return &workload{o.Spec.Template.Spec.Containers, o.Spec.Selector, o.Annotations[helmReleaseAnnotation]}, nil
	case "DaemonSet":
		o, err := kube.AppsV1().DaemonSets(namespace).Get(ctx, ref.Name, get)
		if err != nil {
			return nil, err
		}
		return &workload{o.Spec.Template.Spec.Containers, o.Spec.Selector, o.Annotations[helmReleaseAnnotation]}, nil
	case "ReplicaSet":
		o, err := kube.AppsV1().ReplicaSets(namespace).Get(ctx, ref.Name, get)
		if err != nil {
			return nil, err
		}
		return &workload{o.Spec.Template.Spec.Containers, o.Spec.Selector, o.Annotations[helmReleaseAnnotation]}, nil
	case "Job":
		o, err := kube.BatchV1().Jobs(namespace).Get(ctx, ref.Name, get)
		if err != nil {
			return nil, err
		}
		return &workload{o.Spec.Template.Spec.Containers, o.Spec.Selector, o.Annotations[helmReleaseAnnotation]}, nil
	case "CronJob":
		o, err := kube.BatchV1().CronJobs(namespace).Get(ctx, ref.Name, get)
		if err != nil {
			return nil, err
		}
		return &workload{o.Spec.JobTemplate.Spec.Template.Spec.Containers, nil, o.Annotations[helmReleaseAnnotation]}, nil
	}
	return nil, fmt.Errorf("targetRef kind %q is not supported by the page", ref.Kind)
}

// livePodResources groups the running pods of a workload by what each container
// really has, per container name. Resizing in place or a stale rollout shows up here
// as more than one group.
func livePodResources(ctx context.Context, kube kubernetes.Interface, namespace string, selector *metav1.LabelSelector) (map[string][]livePods, error) {
	if selector == nil {
		return nil, nil
	}
	pods, err := kube.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{LabelSelector: metav1.FormatLabelSelector(selector)})
	if err != nil {
		return nil, err
	}

	type key struct{ container, requests, limits string }
	counts := map[key]*livePods{}
	for i := range pods.Items {
		pod := &pods.Items[i]
		if pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
			continue
		}
		for _, c := range pod.Spec.Containers {
			requests, limits := fromList(c.Resources.Requests), fromList(c.Resources.Limits)
			k := key{c.Name, requests.String(), limits.String()}
			if counts[k] == nil {
				counts[k] = &livePods{Requests: requests, Limits: limits}
			}
			counts[k].Pods++
		}
	}

	out := map[string][]livePods{}
	for k, v := range counts {
		out[k.container] = append(out[k.container], *v)
	}
	for _, groups := range out {
		sort.Slice(groups, func(i, j int) bool { return groups[i].Pods > groups[j].Pods })
	}
	return out, nil
}

// limitRatio is limit/request per resource as the admission controller keeps it when
// it applies a recommendation (pkg/utils/vpa/limit_and_request_scaling.go):
//   - no limit declared: nothing to scale, the resource is absent;
//   - limit declared but no request, or request equal to the limit: 1, the limit
//     follows the request one to one;
//   - otherwise limit/request.
func limitRatio(requests, limits corev1.ResourceList) map[string]float64 {
	ratio := map[string]float64{}
	for _, name := range []corev1.ResourceName{corev1.ResourceCPU, corev1.ResourceMemory} {
		limit, ok := limits[name]
		if !ok || limit.IsZero() {
			continue
		}
		request, ok := requests[name]
		if !ok || request.IsZero() || request.MilliValue() == limit.MilliValue() {
			ratio[string(name)] = 1
			continue
		}
		ratio[string(name)] = float64(limit.MilliValue()) / float64(request.MilliValue())
	}
	if len(ratio) == 0 {
		return nil
	}
	return ratio
}
