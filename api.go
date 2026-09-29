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
	"sort"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/recommender/input/victoriametrics"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/recommender/logic"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/recommender/model"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/recommender/override"
)

// resources is cpu and memory in two forms: the canonical quantity strings for
// display and plain numbers so the page does not have to parse quantities.
type resources struct {
	CPU         string `json:"cpu,omitempty"`
	Memory      string `json:"memory,omitempty"`
	CPUMilli    int64  `json:"cpuMilli,omitempty"`
	MemoryBytes int64  `json:"memoryBytes,omitempty"`
}

func fromList(list corev1.ResourceList) *resources {
	cpu, hasCPU := list[corev1.ResourceCPU]
	memory, hasMemory := list[corev1.ResourceMemory]
	if !hasCPU && !hasMemory {
		return nil
	}
	out := &resources{}
	if hasCPU {
		out.CPU = cpu.String()
		out.CPUMilli = cpu.MilliValue()
	}
	if hasMemory {
		out.Memory = memory.String()
		out.MemoryBytes = memory.Value()
	}
	return out
}

// String is a stable key for grouping equal resources; nil is the empty key.
func (r *resources) String() string {
	if r == nil {
		return ""
	}
	return r.CPU + "/" + r.Memory
}

func fromModel(r model.Resources) *resources {
	list := corev1.ResourceList{}
	if cpu, ok := r[model.ResourceCPU]; ok {
		list[corev1.ResourceCPU] = *resource.NewMilliQuantity(int64(cpu), resource.DecimalSI)
	}
	if memory, ok := r[model.ResourceMemory]; ok {
		list[corev1.ResourceMemory] = *resource.NewQuantity(int64(memory), resource.BinarySI)
	}
	return fromList(list)
}

// recommendation is a target with its lower and upper bound.
type recommendation struct {
	Target     *resources `json:"target,omitempty"`
	LowerBound *resources `json:"lowerBound,omitempty"`
	UpperBound *resources `json:"upperBound,omitempty"`
}

func fromCalculated(c logic.RecommendedContainerResources) *recommendation {
	return &recommendation{Target: fromModel(c.Target), LowerBound: fromModel(c.LowerBound), UpperBound: fromModel(c.UpperBound)}
}

type overrideView struct {
	Mode      string               `json:"mode"`
	Resources map[string]resources `json:"resources,omitempty"`
	SetBy     string               `json:"setBy"`
	SetAt     time.Time            `json:"setAt"`
	ExpiresAt time.Time            `json:"expiresAt"`
	Reason    string               `json:"reason,omitempty"`
	Expired   bool                 `json:"expired"`
}

func viewOverride(o *override.Override, now time.Time) *overrideView {
	if o == nil {
		return nil
	}
	v := &overrideView{
		Mode: string(o.Mode), SetBy: o.SetBy, SetAt: o.SetAt, ExpiresAt: o.ExpiresAt,
		Reason: o.Reason, Expired: o.Expired(now),
	}
	if len(o.Resources) > 0 {
		v.Resources = make(map[string]resources, len(o.Resources))
		for name, list := range o.Resources {
			if r := fromList(list); r != nil {
				v.Resources[name] = *r
			}
		}
	}
	return v
}

type targetRef struct {
	Kind string `json:"kind"`
	Name string `json:"name"`
}

// vpaSummary is a row of the list. It needs nothing but the VPA object itself, so
// listing hundreds of them costs one API call.
type vpaSummary struct {
	Namespace     string                `json:"namespace"`
	Name          string                `json:"name"`
	TargetRef     targetRef             `json:"targetRef"`
	UpdateMode    string                `json:"updateMode"`
	Recommender   string                `json:"recommender"`
	Override      *overrideView         `json:"override,omitempty"`
	OverrideError string                `json:"overrideError,omitempty"`
	Recommended   map[string]*resources `json:"recommended,omitempty"`
}

type livePods struct {
	Requests *resources `json:"requests,omitempty"`
	Limits   *resources `json:"limits,omitempty"`
	Pods     int        `json:"pods"`
}

// containerView is everything the page shows about one container.
type containerView struct {
	Name string `json:"name"`
	// Initial is what the workload manifest (Deployment, Helm chart) declares.
	Initial *initialResources `json:"initial,omitempty"`
	// LimitRatio is limit/request per resource from the manifest. The admission
	// controller keeps this ratio when it applies a recommendation, see
	// pkg/utils/vpa/limit_and_request_scaling.go. Absent: no limit declared.
	LimitRatio map[string]float64 `json:"limitRatio,omitempty"`
	// Live is what the running pods actually have.
	Live []livePods `json:"live,omitempty"`
	// VPA is status.recommendation, what the recommender published.
	VPA *recommendation `json:"vpa,omitempty"`
	// Computed is what the recommender would calculate right now.
	Computed      *recommendation `json:"computed,omitempty"`
	ComputedError string          `json:"computedError,omitempty"`
	// Usage is the consumption right now.
	Usage      *victoriametrics.Usage `json:"usage,omitempty"`
	UsageError string                 `json:"usageError,omitempty"`
	// Pinned is the value a manual override holds for this container.
	Pinned *resources `json:"pinned,omitempty"`
}

type initialResources struct {
	Requests *resources `json:"requests,omitempty"`
	Limits   *resources `json:"limits,omitempty"`
}

type helmInfo struct {
	Release string `json:"release"`
}

type vpaDetail struct {
	vpaSummary
	Helm          *helmInfo       `json:"helm,omitempty"`
	WorkloadError string          `json:"workloadError,omitempty"`
	Containers    []containerView `json:"containers"`
}

type configView struct {
	// User is who the page thinks the visitor is; empty means no writes.
	User       string  `json:"user"`
	ReadOnly   bool    `json:"readOnly"`
	CanWrite   bool    `json:"canWrite"`
	UserHeader string  `json:"userHeader"`
	DefaultTTL float64 `json:"defaultTtlHours"`
	MaxTTL     float64 `json:"maxTtlHours"`
}

// putOverrideRequest is the body of PUT .../override.
type putOverrideRequest struct {
	Mode       string                       `json:"mode"`
	Containers map[string]map[string]string `json:"containers"`
	TTLHours   float64                      `json:"ttlHours"`
	Reason     string                       `json:"reason"`
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
