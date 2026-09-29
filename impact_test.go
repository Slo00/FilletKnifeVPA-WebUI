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
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	vpa_types "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/apis/autoscaling.k8s.io/v1"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/recommender/input/victoriametrics"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/recommender/override"
)

const mi = int64(1 << 20)

// template is the hamster manifest: limit is 10x the request for CPU and 4x for memory.
func template(policies ...corev1.ContainerResizePolicy) corev1.Container {
	return corev1.Container{
		Name:         "hamster",
		Resources:    corev1.ResourceRequirements{Requests: rl("100m", "50Mi"), Limits: rl("1", "200Mi")},
		ResizePolicy: policies,
	}
}

func restartOn(res corev1.ResourceName) corev1.ContainerResizePolicy {
	return corev1.ContainerResizePolicy{ResourceName: res, RestartPolicy: corev1.RestartContainer}
}

// group is n pods that have these requests and limits.
func group(n int, cpu, memory, limitCPU, limitMemory int64) livePods {
	return livePods{
		Pods:     n,
		Requests: &resources{CPUMilli: cpu, MemoryBytes: memory},
		Limits:   &resources{CPUMilli: limitCPU, MemoryBytes: limitMemory},
	}
}

func codes(effects []effect) []string {
	out := make([]string, len(effects))
	for i, e := range effects {
		out[i] = e.Code
	}
	return out
}

func restarts(effects []effect) bool {
	for _, e := range effects {
		if e.Level == levelRestart {
			return true
		}
	}
	return false
}

func TestAnalyze(t *testing.T) {
	initial := group(2, 100, 50*mi, 1000, 200*mi)  // the manifest as deployed
	pinned := group(2, 746, 300*mi, 7460, 1200*mi) // what was pinned before: 4x memory limit
	corridorOK := &corridor{LowerCPU: 600, UpperCPU: 800, LowerMem: 250 * mi, UpperMem: 320 * mi}

	for name, tc := range map[string]struct {
		change      change
		wantCodes   []string
		wantRestart bool
	}{
		"Off applies nothing": {
			change:    change{UpdateMode: vpa_types.UpdateModeOff, Template: template(), Live: []livePods{initial}, NextCPUMilli: 700, NextMemoryBytes: 300 * mi},
			wantCodes: []string{"off"},
		},
		"Initial leaves running pods alone": {
			change:    change{UpdateMode: vpa_types.UpdateModeInitial, Template: template(), Live: []livePods{initial}, NextCPUMilli: 700, NextMemoryBytes: 300 * mi},
			wantCodes: []string{"initial"},
		},
		"Recreate restarts the pods": {
			change:      change{UpdateMode: vpa_types.UpdateModeRecreate, Template: template(), Live: []livePods{initial}, NextCPUMilli: 700, NextMemoryBytes: 300 * mi},
			wantCodes:   []string{"recreate"},
			wantRestart: true,
		},
		"no mode is Auto, which recreates": {
			change:      change{Template: template(), Live: []livePods{initial}, NextCPUMilli: 700, NextMemoryBytes: 300 * mi},
			wantCodes:   []string{"recreate"},
			wantRestart: true,
		},
		"in place raise needs no restart": {
			change:    change{UpdateMode: vpa_types.UpdateModeInPlaceOrRecreate, Template: template(), Live: []livePods{initial}, NextCPUMilli: 700, NextMemoryBytes: 300 * mi},
			wantCodes: []string{"in-place", "capacity"},
		},
		"lowering memory recreates the pods when in place would be refused": {
			change:      change{UpdateMode: vpa_types.UpdateModeInPlaceOrRecreate, Template: template(), Live: []livePods{pinned}, NextCPUMilli: 746, NextMemoryBytes: 250 * mi},
			wantCodes:   []string{"in-place-blocked-recreate"},
			wantRestart: true,
		},
		"lowering memory under InPlace stays unapplied instead of restarting": {
			change:    change{UpdateMode: vpa_types.UpdateModeInPlace, Template: template(), Live: []livePods{pinned}, NextCPUMilli: 746, NextMemoryBytes: 250 * mi},
			wantCodes: []string{"in-place-blocked"},
		},
		"lowering memory with RestartContainer restarts the container, not the pod": {
			change:      change{UpdateMode: vpa_types.UpdateModeInPlace, Template: template(restartOn(corev1.ResourceMemory)), Live: []livePods{pinned}, NextCPUMilli: 746, NextMemoryBytes: 250 * mi},
			wantCodes:   []string{"restart-container"},
			wantRestart: true,
		},
		"a CPU change restarts the container when CPU has RestartContainer": {
			change:      change{UpdateMode: vpa_types.UpdateModeInPlace, Template: template(restartOn(corev1.ResourceCPU)), Live: []livePods{initial}, NextCPUMilli: 200, NextMemoryBytes: 50 * mi},
			wantCodes:   []string{"restart-container", "capacity"},
			wantRestart: true,
		},
		"RestartContainer on memory does not matter when only CPU changes": {
			change:    change{UpdateMode: vpa_types.UpdateModeInPlace, Template: template(restartOn(corev1.ResourceMemory)), Live: []livePods{initial}, NextCPUMilli: 200, NextMemoryBytes: 50 * mi},
			wantCodes: []string{"in-place", "capacity"},
		},
		"a memory limit below the usage gets the container OOM killed": {
			change:      change{UpdateMode: vpa_types.UpdateModeInPlace, Template: template(), Live: []livePods{initial}, NextCPUMilli: 300, NextMemoryBytes: 60 * mi, MemoryUsageBytes: 400 * mi},
			wantCodes:   []string{"oom", "in-place", "capacity"},
			wantRestart: true,
		},
		"a memory limit above the usage is fine": {
			change:    change{UpdateMode: vpa_types.UpdateModeInPlace, Template: template(), Live: []livePods{initial}, NextCPUMilli: 300, NextMemoryBytes: 60 * mi, MemoryUsageBytes: 100 * mi},
			wantCodes: []string{"in-place", "capacity"},
		},
		"the OOM is not predicted for a change that will not be applied": {
			change:    change{UpdateMode: vpa_types.UpdateModeInPlace, Template: template(), Live: []livePods{pinned}, NextCPUMilli: 746, NextMemoryBytes: 250 * mi, MemoryUsageBytes: 1100 * mi},
			wantCodes: []string{"in-place-blocked"},
		},
		"no memory limit in the manifest: lowering the request in place is allowed": {
			change: change{
				UpdateMode:   vpa_types.UpdateModeInPlace,
				Template:     corev1.Container{Name: "hamster", Resources: corev1.ResourceRequirements{Requests: rl("100m", "50Mi")}},
				Live:         []livePods{{Pods: 2, Requests: &resources{CPUMilli: 100, MemoryBytes: 300 * mi}}},
				NextCPUMilli: 100, NextMemoryBytes: 250 * mi,
			},
			wantCodes: []string{"in-place"},
		},
		"pods already there": {
			change:    change{UpdateMode: vpa_types.UpdateModeRecreate, Template: template(), Live: []livePods{pinned}, NextCPUMilli: 746, NextMemoryBytes: 300 * mi},
			wantCodes: []string{"no-change"},
		},
		"inside the corridor the updater leaves the pods": {
			change:    change{UpdateMode: vpa_types.UpdateModeRecreate, Template: template(), Live: []livePods{group(2, 700, 280*mi, 7000, 1120*mi)}, NextCPUMilli: 746, NextMemoryBytes: 300 * mi, Corridor: corridorOK},
			wantCodes: []string{"in-corridor"},
		},
		"outside the corridor the updater acts": {
			change:      change{UpdateMode: vpa_types.UpdateModeRecreate, Template: template(), Live: []livePods{initial}, NextCPUMilli: 746, NextMemoryBytes: 300 * mi, Corridor: corridorOK},
			wantCodes:   []string{"recreate"},
			wantRestart: true,
		},
		"no pods": {
			change:    change{UpdateMode: vpa_types.UpdateModeRecreate, Template: template(), NextCPUMilli: 700, NextMemoryBytes: 300 * mi},
			wantCodes: []string{"no-pods"},
		},
	} {
		t.Run(name, func(t *testing.T) {
			got := analyze(tc.change)
			assert.Equal(t, tc.wantCodes, codes(got), "%+v", got)
			assert.Equal(t, tc.wantRestart, restarts(got))
		})
	}
}

func TestAnalyzeCountsOnlyThePodsThatChange(t *testing.T) {
	got := analyze(change{
		UpdateMode: vpa_types.UpdateModeRecreate, Template: template(),
		Live:         []livePods{group(2, 746, 300*mi, 7460, 1200*mi), group(1, 100, 50*mi, 1000, 200*mi)},
		NextCPUMilli: 746, NextMemoryBytes: 300 * mi,
	})
	require.Len(t, got, 1)
	assert.Contains(t, got[0].Text, "1 из 3 подов")
}

func TestAnalyzeExplainsTheMemoryLimit(t *testing.T) {
	got := analyze(change{
		UpdateMode: vpa_types.UpdateModeInPlaceOrRecreate, Template: template(),
		Live:         []livePods{group(2, 746, 300*mi, 7460, 1200*mi)},
		NextCPUMilli: 746, NextMemoryBytes: 250 * mi,
	})
	require.Len(t, got, 1)
	assert.Contains(t, got[0].Text, "1200Mi → 1000Mi", "the text names the two limits so the person sees why")
	assert.Contains(t, got[0].Text, "RestartContainer")
}

// --- the endpoint -------------------------------------------------------------------------

func setMode(t *testing.T, e *env, mode vpa_types.UpdateMode) {
	t.Helper()
	got, err := e.vpas.AutoscalingV1().VerticalPodAutoscalers("default").Get(context.Background(), "hamster-vpa", metav1.GetOptions{})
	require.NoError(t, err)
	got.Spec.UpdatePolicy = &vpa_types.PodUpdatePolicy{UpdateMode: &mode}
	_, err = e.vpas.AutoscalingV1().VerticalPodAutoscalers("default").Update(context.Background(), got, metav1.UpdateOptions{})
	require.NoError(t, err)
}

func TestImpactEndpoint(t *testing.T) {
	const decrease = `{"mode":"Pinned","containers":{"hamster":{"cpu":"100m","memory":"20Mi"}}}`

	t.Run("Off applies nothing", func(t *testing.T) {
		e := newEnv(t, nil)
		v := decode[impactView](t, e.do("POST", "/api/vpas/default/hamster-vpa/impact", "", `{"mode":"Pinned","containers":{"hamster":{"cpu":"700m","memory":"300Mi"}}}`))
		assert.False(t, v.Restarts)
		require.Len(t, v.Containers, 1)
		assert.Equal(t, []string{"off"}, codes(v.Containers[0].Effects))
	})

	t.Run("Recreate warns about the restart, and needs no identity to ask", func(t *testing.T) {
		e := newEnv(t, nil)
		setMode(t, e, vpa_types.UpdateModeRecreate)
		rec := e.do("POST", "/api/vpas/default/hamster-vpa/impact", "", `{"mode":"Pinned","containers":{"hamster":{"cpu":"700m","memory":"300Mi"}}}`)
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
		v := decode[impactView](t, rec)
		assert.True(t, v.Restarts)
		assert.Equal(t, []string{"recreate"}, codes(v.Containers[0].Effects))
	})

	t.Run("lowering memory in place is called out", func(t *testing.T) {
		e := newEnv(t, nil)
		setMode(t, e, vpa_types.UpdateModeInPlaceOrRecreate)
		// the pods have 50Mi / 200Mi: 20Mi gives a 80Mi limit, below the 200Mi they have
		v := decode[impactView](t, e.do("POST", "/api/vpas/default/hamster-vpa/impact", "", decrease))
		assert.True(t, v.Restarts)
		assert.Equal(t, []string{"in-place-blocked-recreate"}, codes(v.Containers[0].Effects))
	})

	t.Run("a raise in place is quiet", func(t *testing.T) {
		e := newEnv(t, nil)
		setMode(t, e, vpa_types.UpdateModeInPlace)
		v := decode[impactView](t, e.do("POST", "/api/vpas/default/hamster-vpa/impact", "", `{"mode":"Pinned","containers":{"hamster":{"cpu":"700m","memory":"300Mi"}}}`))
		assert.False(t, v.Restarts)
		assert.Equal(t, []string{"in-place", "capacity"}, codes(v.Containers[0].Effects))
	})

	t.Run("returning to the recommender is judged by what it would publish", func(t *testing.T) {
		e := newEnv(t, nil)
		setMode(t, e, vpa_types.UpdateModeRecreate)
		// the fake recommender publishes 700m / 250Mi in the corridor 600m-800m; the pods have 100m
		v := decode[impactView](t, e.do("POST", "/api/vpas/default/hamster-vpa/impact", "", `{"mode":"Auto"}`))
		assert.True(t, v.Restarts)
		assert.Equal(t, []string{"recreate"}, codes(v.Containers[0].Effects))
	})

	t.Run("returning to the recommender when it is unavailable says it cannot tell", func(t *testing.T) {
		e := newEnv(t, nil)
		setMode(t, e, vpa_types.UpdateModeRecreate)
		e.calc.recErr = assert.AnError
		v := decode[impactView](t, e.do("POST", "/api/vpas/default/hamster-vpa/impact", "", `{"mode":"Auto"}`))
		assert.False(t, v.Restarts)
		assert.Equal(t, []string{"no-calculation"}, codes(v.Containers[0].Effects))
	})

	t.Run("a memory limit below the usage kills the container", func(t *testing.T) {
		e := newEnv(t, nil)
		setMode(t, e, vpa_types.UpdateModeInPlace)
		e.calc.usage = victoriametrics.Usage{Pods: 2, MemoryBytesMax: float64(600 * mi)}
		// 300Mi of memory gives a 1200Mi limit; 100Mi gives 400Mi, below the 600Mi in use
		v := decode[impactView](t, e.do("POST", "/api/vpas/default/hamster-vpa/impact", "", `{"mode":"Pinned","containers":{"hamster":{"cpu":"700m","memory":"100Mi"}}}`))
		assert.True(t, v.Restarts)
		assert.Contains(t, codes(v.Containers[0].Effects), "oom")
	})

	t.Run("Paused leaves the pods alone", func(t *testing.T) {
		e := newEnv(t, nil)
		setMode(t, e, vpa_types.UpdateModeRecreate)
		v := decode[impactView](t, e.do("POST", "/api/vpas/default/hamster-vpa/impact", "", `{"mode":"Paused"}`))
		assert.False(t, v.Restarts)
		assert.Equal(t, []string{"paused"}, codes(v.Containers[0].Effects))
	})

	t.Run("asking writes nothing", func(t *testing.T) {
		e := newEnv(t, nil)
		setMode(t, e, vpa_types.UpdateModeRecreate)
		e.do("POST", "/api/vpas/default/hamster-vpa/impact", "alice", `{"mode":"Pinned","containers":{"hamster":{"cpu":"700m","memory":"300Mi"}}}`)
		assert.Empty(t, e.vpaAnnotations(t)[override.AnnotationMode])
		assert.Empty(t, e.events(t))
	})
}

func TestImpactEndpointRejects(t *testing.T) {
	for name, tc := range map[string]struct {
		path string
		body string
		want int
	}{
		"bad json":          {body: `{`, want: http.StatusBadRequest},
		"unknown field":     {body: `{"mode":"Auto","x":1}`, want: http.StatusBadRequest},
		"bad mode":          {body: `{"mode":"Fast"}`, want: http.StatusBadRequest},
		"unknown container": {body: `{"mode":"Pinned","containers":{"nope":{"cpu":"1","memory":"1Gi"}}}`, want: http.StatusBadRequest},
		"bad quantity":      {body: `{"mode":"Pinned","containers":{"hamster":{"cpu":"lots","memory":"1Gi"}}}`, want: http.StatusBadRequest},
		"cpu only":          {body: `{"mode":"Pinned","containers":{"hamster":{"cpu":"1"}}}`, want: http.StatusBadRequest},
		"memory only":       {body: `{"mode":"Pinned","containers":{"hamster":{"memory":"1Gi"}}}`, want: http.StatusBadRequest},
		"unknown vpa":       {path: "/api/vpas/default/nope/impact", body: `{"mode":"Auto"}`, want: http.StatusNotFound},
	} {
		t.Run(name, func(t *testing.T) {
			e := newEnv(t, nil)
			path := tc.path
			if path == "" {
				path = "/api/vpas/default/hamster-vpa/impact"
			}
			assert.Equal(t, tc.want, e.do("POST", path, "", tc.body).Code)
		})
	}
}

func TestImpactWithoutTheWorkloadSaysSo(t *testing.T) {
	e := newEnv(t, nil)
	require.NoError(t, e.kube.AppsV1().Deployments("default").Delete(context.Background(), "hamster", metav1.DeleteOptions{}))
	assert.Equal(t, http.StatusBadGateway, e.do("POST", "/api/vpas/default/hamster-vpa/impact", "", `{"mode":"Auto"}`).Code)
}
