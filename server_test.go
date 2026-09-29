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
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	autoscalingv1 "k8s.io/api/autoscaling/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	kubefake "k8s.io/client-go/kubernetes/fake"

	vpa_types "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/apis/autoscaling.k8s.io/v1"
	vpafake "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/client/clientset/versioned/fake"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/recommender/input/victoriametrics"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/recommender/logic"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/recommender/model"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/recommender/override"
	vpa_api_util "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/utils/vpa"
)

var fixedNow = time.Date(2026, 9, 19, 12, 0, 0, 0, time.UTC)

type fakeCalc struct {
	recommended logic.RecommendedPodResources
	recErr      error
	usage       victoriametrics.Usage
	usageErr    error
	history     victoriametrics.History
	historyErr  map[string]error
	historyCall struct {
		container string
		start     time.Time
		end       time.Time
		step      time.Duration
	}
}

func (f *fakeCalc) GetRecommendedPodResources(_ context.Context, _, _ string, _ []string) (logic.RecommendedPodResources, error) {
	return f.recommended, f.recErr
}

func (f *fakeCalc) GetHistory(_ context.Context, _, _, container string, start, end time.Time, step time.Duration) (victoriametrics.History, error) {
	f.historyCall.container, f.historyCall.start, f.historyCall.end, f.historyCall.step = container, start, end, step
	if err := f.historyErr[container]; err != nil {
		return victoriametrics.History{}, err
	}
	return f.history, nil
}

func (f *fakeCalc) GetCurrentUsage(_ context.Context, _, _, _ string) (victoriametrics.Usage, error) {
	return f.usage, f.usageErr
}

func rl(cpu, memory string) corev1.ResourceList {
	return corev1.ResourceList{corev1.ResourceCPU: resource.MustParse(cpu), corev1.ResourceMemory: resource.MustParse(memory)}
}

func hamsterDeployment() *appsv1.Deployment {
	labels := map[string]string{"app": "hamster"}
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "hamster", Namespace: "default", Annotations: map[string]string{helmReleaseAnnotation: "hamster-release"}},
		Spec: appsv1.DeploymentSpec{
			Selector: &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{Containers: []corev1.Container{{
					Name:      "hamster",
					Resources: corev1.ResourceRequirements{Requests: rl("100m", "50Mi"), Limits: rl("1", "200Mi")},
				}}},
			},
		},
	}
}

func hamsterPod(name string, requests, limits corev1.ResourceList) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default", Labels: map[string]string{"app": "hamster"}},
		Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "hamster", Resources: corev1.ResourceRequirements{Requests: requests, Limits: limits}}}},
		Status:     corev1.PodStatus{Phase: corev1.PodRunning},
	}
}

func hamsterVPA(annotations map[string]string) *vpa_types.VerticalPodAutoscaler {
	off := vpa_types.UpdateModeOff
	return &vpa_types.VerticalPodAutoscaler{
		ObjectMeta: metav1.ObjectMeta{Name: "hamster-vpa", Namespace: "default", UID: "uid-1", Annotations: annotations},
		Spec: vpa_types.VerticalPodAutoscalerSpec{
			TargetRef:    &autoscalingv1.CrossVersionObjectReference{APIVersion: "apps/v1", Kind: "Deployment", Name: "hamster"},
			UpdatePolicy: &vpa_types.PodUpdatePolicy{UpdateMode: &off},
			Recommenders: []*vpa_types.VerticalPodAutoscalerRecommenderSelector{{Name: "vm"}},
		},
		Status: vpa_types.VerticalPodAutoscalerStatus{Recommendation: &vpa_types.RecommendedPodResources{
			ContainerRecommendations: []vpa_types.RecommendedContainerResources{{
				ContainerName: "hamster", Target: rl("748m", "250Mi"), LowerBound: rl("588m", "250Mi"), UpperBound: rl("825m", "250Mi"),
			}},
		}},
	}
}

type env struct {
	srv  *Server
	kube *kubefake.Clientset
	vpas *vpafake.Clientset
	calc *fakeCalc
}

func newEnv(t *testing.T, edit func(*Options), objs ...runtime.Object) *env {
	t.Helper()
	kube := kubefake.NewClientset(append([]runtime.Object{
		hamsterDeployment(),
		hamsterPod("hamster-1", rl("100m", "50Mi"), rl("1", "200Mi")),
		hamsterPod("hamster-2", rl("100m", "50Mi"), rl("1", "200Mi")),
	}, objs...)...)
	vpas := vpafake.NewSimpleClientset(hamsterVPA(nil))
	calc := &fakeCalc{
		recommended: logic.RecommendedPodResources{"hamster": logic.RecommendedContainerResources{
			Target:     model.Resources{model.ResourceCPU: 700, model.ResourceMemory: 262144000},
			LowerBound: model.Resources{model.ResourceCPU: 600, model.ResourceMemory: 262144000},
			UpperBound: model.Resources{model.ResourceCPU: 800, model.ResourceMemory: 262144000},
		}},
		usage: victoriametrics.Usage{Pods: 2, CPUCoresAvg: 0.53, CPUCoresMax: 0.9, MemoryBytesAvg: 3 << 20, MemoryBytesMax: 4 << 20},
	}
	opts := Options{UserHeader: "X-Forwarded-User", DefaultTTL: 7 * 24 * time.Hour, MaxTTL: 30 * 24 * time.Hour, QueryTimeout: time.Second}
	if edit != nil {
		edit(&opts)
	}
	srv := NewServer(kube, vpas, calc, opts)
	srv.now = func() time.Time { return fixedNow }
	return &env{srv, kube, vpas, calc}
}

func (e *env) do(method, path, user, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	if user != "" {
		req.Header.Set("X-Forwarded-User", user)
	}
	rec := httptest.NewRecorder()
	e.srv.Handler().ServeHTTP(rec, req)
	return rec
}

func decode[T any](t *testing.T, rec *httptest.ResponseRecorder) T {
	t.Helper()
	var v T
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &v), rec.Body.String())
	return v
}

func (e *env) vpaAnnotations(t *testing.T) map[string]string {
	t.Helper()
	got, err := e.vpas.AutoscalingV1().VerticalPodAutoscalers("default").Get(context.Background(), "hamster-vpa", metav1.GetOptions{})
	require.NoError(t, err)
	return got.Annotations
}

func (e *env) events(t *testing.T) []corev1.Event {
	t.Helper()
	list, err := e.kube.CoreV1().Events("default").List(context.Background(), metav1.ListOptions{})
	require.NoError(t, err)
	return list.Items
}

const pinBody = `{"mode":"Pinned","containers":{"hamster":{"cpu":"700m","memory":"300Mi"}},"ttlHours":72,"reason":"release freeze"}`

func TestConfigIdentity(t *testing.T) {
	e := newEnv(t, nil)

	cfg := decode[configView](t, e.do("GET", "/api/config", "alice", ""))
	assert.Equal(t, "alice", cfg.User)
	assert.True(t, cfg.CanWrite)

	cfg = decode[configView](t, e.do("GET", "/api/config", "", ""))
	assert.Empty(t, cfg.User)
	assert.False(t, cfg.CanWrite, "no identity, no writes")

	e = newEnv(t, func(o *Options) { o.ReadOnly = true })
	cfg = decode[configView](t, e.do("GET", "/api/config", "alice", ""))
	assert.False(t, cfg.CanWrite)

	e = newEnv(t, func(o *Options) { o.DevUser = "dev" })
	cfg = decode[configView](t, e.do("GET", "/api/config", "", ""))
	assert.Equal(t, "dev", cfg.User, "the dev user stands in when the header is absent")
	cfg = decode[configView](t, e.do("GET", "/api/config", "alice", ""))
	assert.Equal(t, "alice", cfg.User, "the header wins over the dev user")
}

func TestList(t *testing.T) {
	pinned, err := (&override.Override{
		Mode: override.ModePinned, Resources: map[string]corev1.ResourceList{"hamster": rl("700m", "300Mi")},
		SetBy: "alice", SetAt: fixedNow.Add(-time.Hour), ExpiresAt: fixedNow.Add(time.Hour),
	}).ToAnnotations()
	require.NoError(t, err)
	other := hamsterVPA(pinned)
	other.Name = "other-vpa"
	other.Spec.Recommenders = nil
	other.Spec.UpdatePolicy = nil

	e := newEnv(t, nil)
	_, err = e.vpas.AutoscalingV1().VerticalPodAutoscalers("default").Create(context.Background(), other, metav1.CreateOptions{})
	require.NoError(t, err)

	rec := e.do("GET", "/api/vpas", "", "")
	require.Equal(t, http.StatusOK, rec.Code)
	items := decode[struct{ Items []vpaSummary }](t, rec).Items
	require.Len(t, items, 2)

	byName := map[string]vpaSummary{}
	for _, it := range items {
		byName[it.Name] = it
	}
	auto := byName["hamster-vpa"]
	assert.Nil(t, auto.Override)
	assert.Equal(t, "vm", auto.Recommender)
	assert.Equal(t, "Off", auto.UpdateMode)
	assert.Equal(t, "Deployment", auto.TargetRef.Kind)
	assert.Equal(t, int64(748), auto.Recommended["hamster"].CPUMilli)

	p := byName["other-vpa"]
	require.NotNil(t, p.Override)
	assert.Equal(t, "Pinned", p.Override.Mode)
	assert.Equal(t, "alice", p.Override.SetBy)
	assert.False(t, p.Override.Expired)
	assert.Equal(t, "default", p.Recommender, "no recommenders listed means the default one")
	assert.Equal(t, "Auto", p.UpdateMode)
}

func TestListShowsBrokenOverride(t *testing.T) {
	broken := hamsterVPA(map[string]string{override.AnnotationMode: "Pinned"})
	e := newEnv(t, nil)
	_, err := e.vpas.AutoscalingV1().VerticalPodAutoscalers("default").Update(context.Background(), broken, metav1.UpdateOptions{})
	require.NoError(t, err)

	items := decode[struct{ Items []vpaSummary }](t, e.do("GET", "/api/vpas", "", "")).Items
	require.Len(t, items, 1)
	assert.NotEmpty(t, items[0].OverrideError, "a broken override is reported, not hidden")
	assert.Nil(t, items[0].Override)
}

func TestNamespaceScope(t *testing.T) {
	e := newEnv(t, func(o *Options) { o.Namespace = "team-a" })
	assert.Equal(t, http.StatusForbidden, e.do("GET", "/api/vpas?namespace=default", "", "").Code)
	assert.Equal(t, http.StatusForbidden, e.do("GET", "/api/vpas/default/hamster-vpa", "", "").Code)
	assert.Equal(t, http.StatusForbidden, e.do("PUT", "/api/vpas/default/hamster-vpa/override", "alice", pinBody).Code)
	assert.Empty(t, e.vpaAnnotations(t)[override.AnnotationMode])
}

func TestDetail(t *testing.T) {
	e := newEnv(t, nil)
	rec := e.do("GET", "/api/vpas/default/hamster-vpa", "", "")
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	d := decode[vpaDetail](t, rec)

	require.NotNil(t, d.Helm)
	assert.Equal(t, "hamster-release", d.Helm.Release)
	assert.Empty(t, d.WorkloadError)
	require.Len(t, d.Containers, 1)

	c := d.Containers[0]
	assert.Equal(t, "hamster", c.Name)
	require.NotNil(t, c.Initial)
	assert.Equal(t, int64(100), c.Initial.Requests.CPUMilli, "the manifest's own request")
	assert.Equal(t, int64(1000), c.Initial.Limits.CPUMilli)
	assert.Equal(t, map[string]float64{"cpu": 10, "memory": 4}, c.LimitRatio)

	require.Len(t, c.Live, 1, "both pods have the same resources")
	assert.Equal(t, 2, c.Live[0].Pods)
	assert.Equal(t, int64(100), c.Live[0].Requests.CPUMilli)

	require.NotNil(t, c.VPA)
	assert.Equal(t, int64(748), c.VPA.Target.CPUMilli)
	assert.Equal(t, int64(588), c.VPA.LowerBound.CPUMilli)
	require.NotNil(t, c.Computed)
	assert.Equal(t, int64(700), c.Computed.Target.CPUMilli)
	assert.Equal(t, int64(262144000), c.Computed.Target.MemoryBytes)
	require.NotNil(t, c.Usage)
	assert.Equal(t, 2, c.Usage.Pods)
	assert.InDelta(t, 0.53, c.Usage.CPUCoresAvg, 1e-9)
	assert.Nil(t, c.Pinned)
}

func TestDetailGroupsPodsWithDifferentResources(t *testing.T) {
	resized := hamsterPod("hamster-3", rl("700m", "300Mi"), rl("7", "1200Mi"))
	e := newEnv(t, nil, resized)
	c := decode[vpaDetail](t, e.do("GET", "/api/vpas/default/hamster-vpa", "", "")).Containers[0]
	require.Len(t, c.Live, 2)
	assert.Equal(t, 2, c.Live[0].Pods, "the bigger group first")
	assert.Equal(t, 1, c.Live[1].Pods)
	assert.Equal(t, int64(7000), c.Live[1].Limits.CPUMilli)
}

func TestDetailSurvivesVictoriaMetricsBeingDown(t *testing.T) {
	e := newEnv(t, nil)
	e.calc.recErr = errors.New("connection refused")
	e.calc.usageErr = errors.New("connection refused")

	rec := e.do("GET", "/api/vpas/default/hamster-vpa", "", "")
	require.Equal(t, http.StatusOK, rec.Code, "a broken metrics store must not hide the manifest and the pods")
	c := decode[vpaDetail](t, rec).Containers[0]
	assert.Contains(t, c.ComputedError, "connection refused")
	assert.Contains(t, c.UsageError, "connection refused")
	assert.Nil(t, c.Computed)
	assert.Nil(t, c.Usage)
	require.NotNil(t, c.Initial)
	assert.Len(t, c.Live, 1)
	assert.NotNil(t, c.VPA)
}

func TestDetailWithoutWorkloadFallsBackToRecommendation(t *testing.T) {
	e := newEnv(t, nil)
	require.NoError(t, e.kube.AppsV1().Deployments("default").Delete(context.Background(), "hamster", metav1.DeleteOptions{}))

	d := decode[vpaDetail](t, e.do("GET", "/api/vpas/default/hamster-vpa", "", ""))
	assert.NotEmpty(t, d.WorkloadError)
	require.Len(t, d.Containers, 1, "containers still come from the published recommendation")
	assert.Nil(t, d.Containers[0].Initial)
	assert.NotNil(t, d.Containers[0].VPA)
}

func TestDetailNotFound(t *testing.T) {
	e := newEnv(t, nil)
	assert.Equal(t, http.StatusNotFound, e.do("GET", "/api/vpas/default/nope", "", "").Code)
}

func TestPutOverride(t *testing.T) {
	e := newEnv(t, nil)
	rec := e.do("PUT", "/api/vpas/default/hamster-vpa/override", "alice", pinBody)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	ov, err := override.FromAnnotations(e.vpaAnnotations(t))
	require.NoError(t, err)
	require.NotNil(t, ov)
	assert.Equal(t, override.ModePinned, ov.Mode)
	assert.Equal(t, "alice", ov.SetBy, "the author comes from the header")
	assert.True(t, fixedNow.Equal(ov.SetAt))
	assert.True(t, fixedNow.Add(72*time.Hour).Equal(ov.ExpiresAt))
	assert.Equal(t, "release freeze", ov.Reason)
	cpu, memory := ov.Quantities("hamster")
	require.NotNil(t, cpu)
	assert.Equal(t, int64(700), cpu.MilliValue())
	assert.Equal(t, int64(300<<20), memory.Value())

	events := e.events(t)
	require.Len(t, events, 1)
	assert.Equal(t, "RecommendationOverrideSet", events[0].Reason)
	assert.Contains(t, events[0].Message, "alice")
	assert.Equal(t, "hamster-vpa", events[0].InvolvedObject.Name)
	assert.Equal(t, "VerticalPodAutoscaler", events[0].InvolvedObject.Kind)

	d := decode[vpaDetail](t, e.do("GET", "/api/vpas/default/hamster-vpa", "", ""))
	require.NotNil(t, d.Override)
	require.NotNil(t, d.Containers[0].Pinned)
	assert.Equal(t, int64(700), d.Containers[0].Pinned.CPUMilli)
}

func TestPutOverrideDefaultTTL(t *testing.T) {
	e := newEnv(t, nil)
	body := `{"mode":"Pinned","containers":{"hamster":{"cpu":"700m","memory":"300Mi"}}}`
	require.Equal(t, http.StatusOK, e.do("PUT", "/api/vpas/default/hamster-vpa/override", "alice", body).Code)
	ov, err := override.FromAnnotations(e.vpaAnnotations(t))
	require.NoError(t, err)
	assert.True(t, fixedNow.Add(7*24*time.Hour).Equal(ov.ExpiresAt), "an override always gets an expiry")
}

func TestPausedReplacesPinnedAndDropsItsValues(t *testing.T) {
	e := newEnv(t, nil)
	require.Equal(t, http.StatusOK, e.do("PUT", "/api/vpas/default/hamster-vpa/override", "alice", pinBody).Code)
	require.Equal(t, http.StatusOK, e.do("PUT", "/api/vpas/default/hamster-vpa/override", "bob", `{"mode":"Paused","ttlHours":2}`).Code)

	annotations := e.vpaAnnotations(t)
	assert.NotContains(t, annotations, override.AnnotationResources, "stale pinned values must not survive a switch to Paused")
	ov, err := override.FromAnnotations(annotations)
	require.NoError(t, err)
	assert.Equal(t, override.ModePaused, ov.Mode)
	assert.Equal(t, "bob", ov.SetBy)

	events := e.events(t)
	require.Len(t, events, 2)
	var replaced bool
	for _, ev := range events {
		replaced = replaced || strings.Contains(ev.Message, "replaces the Pinned set by alice")
	}
	assert.True(t, replaced, "the event says whose override was replaced")
}

func TestPutOverrideRejects(t *testing.T) {
	for name, tc := range map[string]struct {
		user string
		edit func(*Options)
		path string
		body string
		want int
	}{
		"no identity":        {user: "", body: pinBody, want: http.StatusForbidden},
		"read only":          {user: "alice", edit: func(o *Options) { o.ReadOnly = true }, body: pinBody, want: http.StatusForbidden},
		"unknown vpa":        {user: "alice", path: "/api/vpas/default/nope/override", body: pinBody, want: http.StatusNotFound},
		"bad json":           {user: "alice", body: `{`, want: http.StatusBadRequest},
		"unknown field":      {user: "alice", body: `{"mode":"Paused","nope":1}`, want: http.StatusBadRequest},
		"bad mode":           {user: "alice", body: `{"mode":"Fast","ttlHours":1}`, want: http.StatusBadRequest},
		"auto is not a mode": {user: "alice", body: `{"mode":"Auto","ttlHours":1}`, want: http.StatusBadRequest},
		"unknown container":  {user: "alice", body: `{"mode":"Pinned","containers":{"nope":{"cpu":"1","memory":"1Gi"}},"ttlHours":1}`, want: http.StatusBadRequest},
		"bad quantity":       {user: "alice", body: `{"mode":"Pinned","containers":{"hamster":{"cpu":"lots","memory":"1Gi"}},"ttlHours":1}`, want: http.StatusBadRequest},
		"cpu only":           {user: "alice", body: `{"mode":"Pinned","containers":{"hamster":{"cpu":"1"}},"ttlHours":1}`, want: http.StatusBadRequest},
		"zero memory":        {user: "alice", body: `{"mode":"Pinned","containers":{"hamster":{"cpu":"1","memory":"0"}},"ttlHours":1}`, want: http.StatusBadRequest},
		"other resource":     {user: "alice", body: `{"mode":"Pinned","containers":{"hamster":{"cpu":"1","memory":"1Gi","gpu":"1"}},"ttlHours":1}`, want: http.StatusBadRequest},
		"nothing pinned":     {user: "alice", body: `{"mode":"Pinned","ttlHours":1}`, want: http.StatusBadRequest},
		"paused with values": {user: "alice", body: `{"mode":"Paused","containers":{"hamster":{"cpu":"1","memory":"1Gi"}},"ttlHours":1}`, want: http.StatusBadRequest},
		"too long":           {user: "alice", body: `{"mode":"Pinned","containers":{"hamster":{"cpu":"1","memory":"1Gi"}},"ttlHours":721}`, want: http.StatusBadRequest},
		"negative ttl":       {user: "alice", body: `{"mode":"Pinned","containers":{"hamster":{"cpu":"1","memory":"1Gi"}},"ttlHours":-1}`, want: http.StatusBadRequest},
	} {
		t.Run(name, func(t *testing.T) {
			e := newEnv(t, tc.edit)
			path := tc.path
			if path == "" {
				path = "/api/vpas/default/hamster-vpa/override"
			}
			rec := e.do("PUT", path, tc.user, tc.body)
			assert.Equal(t, tc.want, rec.Code, rec.Body.String())
			assert.Contains(t, rec.Body.String(), `"error"`)
			assert.Empty(t, e.vpaAnnotations(t)[override.AnnotationMode], "a rejected request must not touch the VPA")
			assert.Empty(t, e.events(t))
		})
	}
}

func TestDeleteOverride(t *testing.T) {
	e := newEnv(t, nil)
	require.Equal(t, http.StatusOK, e.do("PUT", "/api/vpas/default/hamster-vpa/override", "alice", pinBody).Code)
	require.NotEmpty(t, e.vpaAnnotations(t)[override.AnnotationMode])

	rec := e.do("DELETE", "/api/vpas/default/hamster-vpa/override", "bob", "")
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	for _, key := range override.AnnotationKeys() {
		assert.NotContains(t, e.vpaAnnotations(t), key)
	}
	var cleared *corev1.Event
	for i, ev := range e.events(t) {
		if ev.Reason == "RecommendationOverrideCleared" {
			cleared = &e.events(t)[i]
		}
	}
	require.NotNil(t, cleared, "clearing leaves a trail, the annotations are gone")
	assert.Contains(t, cleared.Message, "bob")
	assert.Contains(t, cleared.Message, "alice", "and it says whose override was removed")
}

func TestDeleteRequiresWriter(t *testing.T) {
	e := newEnv(t, nil)
	assert.Equal(t, http.StatusForbidden, e.do("DELETE", "/api/vpas/default/hamster-vpa/override", "", "").Code)
}

func TestDeleteClearsBrokenOverride(t *testing.T) {
	e := newEnv(t, nil)
	_, err := e.vpas.AutoscalingV1().VerticalPodAutoscalers("default").Update(context.Background(),
		hamsterVPA(map[string]string{override.AnnotationMode: "Pinned", "unrelated": "keep"}), metav1.UpdateOptions{})
	require.NoError(t, err)

	require.Equal(t, http.StatusOK, e.do("DELETE", "/api/vpas/default/hamster-vpa/override", "alice", "").Code)
	annotations := e.vpaAnnotations(t)
	assert.NotContains(t, annotations, override.AnnotationMode)
	assert.Equal(t, "keep", annotations["unrelated"], "only the override's own annotations are touched")
}

func TestServesThePage(t *testing.T) {
	e := newEnv(t, nil)
	rec := e.do("GET", "/", "", "")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "<title>FilletKnifeVPA")
	assert.Equal(t, "no-store", e.do("GET", "/api/config", "", "").Header().Get("Cache-Control"))
	assert.Equal(t, http.StatusOK, e.do("GET", "/healthz", "", "").Code)
}

// The page shows the limit the admission controller will set from the manifest's
// proportion. limitRatio must agree with the real code, or the page would lie.
func TestLimitRatioAgreesWithAdmissionController(t *testing.T) {
	for name, tc := range map[string]struct{ requests, limits corev1.ResourceList }{
		"10x":                   {rl("100m", "50Mi"), rl("1", "200Mi")},
		"4x":                    {rl("250m", "128Mi"), rl("1", "512Mi")},
		"equal":                 {rl("500m", "256Mi"), rl("500m", "256Mi")},
		"limit without request": {corev1.ResourceList{}, rl("500m", "256Mi")},
		"fractional":            {rl("300m", "100Mi"), rl("700m", "300Mi")},
	} {
		t.Run(name, func(t *testing.T) {
			recommended := rl("777m", "333Mi")
			want, _ := vpa_api_util.GetProportionalLimit(tc.limits, tc.requests, recommended, corev1.ResourceList{})
			ratio := limitRatio(tc.requests, tc.limits)

			cpu := float64(recommended.Cpu().MilliValue()) * ratio["cpu"]
			assert.InDelta(t, float64(want.Cpu().MilliValue()), cpu, 1, "cpu")
			memory := float64(recommended.Memory().MilliValue()) * ratio["memory"]
			assert.InDelta(t, float64(want.Memory().MilliValue()), math.Round(memory), 1000, "memory")
		})
	}

	assert.Nil(t, limitRatio(rl("100m", "50Mi"), corev1.ResourceList{}), "no limit declared, nothing to scale")
	assert.Nil(t, limitRatio(nil, nil))
}

func TestHistoryStep(t *testing.T) {
	for period, want := range map[time.Duration]time.Duration{
		time.Hour:          30 * time.Second,
		6 * time.Hour:      90 * time.Second,
		24 * time.Hour:     6 * time.Minute,
		3 * 24 * time.Hour: 18 * time.Minute,
		7 * 24 * time.Hour: 42 * time.Minute,
		time.Minute:        30 * time.Second,
	} {
		assert.Equal(t, want, historyStep(period), period.String())
		assert.Zero(t, historyStep(period)%(30*time.Second), "steps are whole multiples of the scrape interval")
	}
}

func TestHistory(t *testing.T) {
	e := newEnv(t, nil)
	e.calc.history = victoriametrics.History{
		CPU:    victoriametrics.ResourceHistory{Usage: victoriametrics.Series{{1000, 0.5}, {1090, 0.6}}, Requests: victoriametrics.Series{{1000, 0.1}}},
		Memory: victoriametrics.ResourceHistory{Limits: victoriametrics.Series{{1000, 209715200}}},
	}

	rec := e.do("GET", "/api/vpas/default/hamster-vpa/history?range=24h", "", "")
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	v := decode[historyView](t, rec)

	assert.Equal(t, "24h", v.Range)
	assert.Equal(t, fixedNow.Unix(), v.End)
	assert.Equal(t, fixedNow.Add(-24*time.Hour).Unix(), v.Start)
	assert.Equal(t, int64(360), v.StepSeconds)
	require.Len(t, v.Containers, 1)
	assert.Equal(t, "hamster", v.Containers[0].Name)
	require.NotNil(t, v.Containers[0].History)
	assert.Equal(t, victoriametrics.Series{{1000, 0.5}, {1090, 0.6}}, v.Containers[0].History.CPU.Usage)
	assert.Equal(t, victoriametrics.Series{{1000, 209715200}}, v.Containers[0].History.Memory.Limits)
	assert.Empty(t, v.Containers[0].History.Memory.Requests, "a series VictoriaMetrics does not have stays empty")

	assert.Equal(t, "hamster", e.calc.historyCall.container)
	assert.Equal(t, 6*time.Minute, e.calc.historyCall.step)
	assert.True(t, e.calc.historyCall.end.Sub(e.calc.historyCall.start) == 24*time.Hour)
}

func TestHistoryDefaultRange(t *testing.T) {
	e := newEnv(t, nil)
	v := decode[historyView](t, e.do("GET", "/api/vpas/default/hamster-vpa/history", "", ""))
	assert.Equal(t, "6h", v.Range)
}

func TestHistoryRejectsUnlistedRanges(t *testing.T) {
	e := newEnv(t, nil)
	for _, rng := range []string{"30d", "1y", "abc", "-1h", "0"} {
		rec := e.do("GET", "/api/vpas/default/hamster-vpa/history?range="+rng, "", "")
		assert.Equal(t, http.StatusBadRequest, rec.Code, rng)
	}
}

func TestHistoryReportsAContainerThatFailed(t *testing.T) {
	e := newEnv(t, nil)
	e.calc.historyErr = map[string]error{"hamster": errors.New("connection refused")}

	rec := e.do("GET", "/api/vpas/default/hamster-vpa/history", "", "")
	require.Equal(t, http.StatusOK, rec.Code, "a broken metrics store is reported per container, not as a failed page")
	v := decode[historyView](t, rec)
	require.Len(t, v.Containers, 1)
	assert.Contains(t, v.Containers[0].Error, "connection refused")
	assert.Nil(t, v.Containers[0].History)
}

func TestHistoryUnknownVPA(t *testing.T) {
	e := newEnv(t, nil)
	assert.Equal(t, http.StatusNotFound, e.do("GET", "/api/vpas/default/nope/history", "", "").Code)
}
