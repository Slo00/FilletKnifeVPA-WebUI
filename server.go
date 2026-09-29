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
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"

	vpa_types "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/apis/autoscaling.k8s.io/v1"
	vpa_clientset "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/client/clientset/versioned"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/recommender/input/victoriametrics"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/recommender/logic"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/recommender/override"
)

//go:embed static
var staticFiles embed.FS

// calculator is what the page needs from VictoriaMetrics; *victoriametrics.Recommender
// is the real one, so the numbers are the recommender's own.
type calculator interface {
	GetRecommendedPodResources(ctx context.Context, namespace, podNamePrefix string, containerNames []string) (logic.RecommendedPodResources, error)
	GetCurrentUsage(ctx context.Context, namespace, podNamePrefix, containerName string) (victoriametrics.Usage, error)
	GetHistory(ctx context.Context, namespace, podNamePrefix, containerName string, start, end time.Time, step time.Duration) (victoriametrics.History, error)
}

// Options are the settings of the page itself.
type Options struct {
	// Namespace restricts the page to one namespace; empty is all of them.
	Namespace string
	// ReadOnly turns every write off.
	ReadOnly bool
	// UserHeader is the header the SSO proxy in front of the page puts the user name in.
	UserHeader string
	// DevUser is used when the header is absent. For local runs only.
	DevUser    string
	DefaultTTL time.Duration
	MaxTTL     time.Duration
	// QueryTimeout bounds the VictoriaMetrics queries behind one page.
	QueryTimeout time.Duration
}

const (
	eventSource         = "vpa-ui"
	maxRequestBodyBytes = 64 << 10
)

// Server serves the API and the page.
type Server struct {
	kube kubernetes.Interface
	vpas vpa_clientset.Interface
	calc calculator
	opts Options
	now  func() time.Time
	// eventSeq keeps event names unique even when two share a timestamp.
	eventSeq atomic.Uint64
}

// NewServer wires the server up.
func NewServer(kube kubernetes.Interface, vpas vpa_clientset.Interface, calc calculator, opts Options) *Server {
	return &Server{kube: kube, vpas: vpas, calc: calc, opts: opts, now: time.Now}
}

// Handler returns the routes. PUT and DELETE are not "simple" requests for a browser,
// so a page on another origin cannot fire them without a CORS preflight, which is
// never answered: this is what keeps a header-authenticated API safe from CSRF.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok")) })
	mux.HandleFunc("GET /api/config", s.handleConfig)
	mux.HandleFunc("GET /api/vpas", s.handleList)
	mux.HandleFunc("GET /api/vpas/{namespace}/{name}", s.handleDetail)
	mux.HandleFunc("GET /api/vpas/{namespace}/{name}/history", s.handleHistory)
	mux.HandleFunc("POST /api/vpas/{namespace}/{name}/impact", s.handleImpact)
	mux.HandleFunc("PUT /api/vpas/{namespace}/{name}/override", s.handlePutOverride)
	mux.HandleFunc("DELETE /api/vpas/{namespace}/{name}/override", s.handleDeleteOverride)

	static, err := fs.Sub(staticFiles, "static")
	if err != nil {
		panic(err)
	}
	mux.Handle("/", http.FileServerFS(static))
	return noStoreAPI(mux)
}

func noStoreAPI(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/api/") {
			w.Header().Set("Cache-Control", "no-store")
		}
		next.ServeHTTP(w, r)
	})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		klog.ErrorS(err, "Cannot write response")
	}
}

func writeError(w http.ResponseWriter, status int, format string, args ...any) {
	writeJSON(w, status, map[string]string{"error": fmt.Sprintf(format, args...)})
}

func (s *Server) user(r *http.Request) string {
	if u := strings.TrimSpace(r.Header.Get(s.opts.UserHeader)); u != "" {
		return u
	}
	return s.opts.DevUser
}

func (s *Server) handleConfig(w http.ResponseWriter, r *http.Request) {
	user := s.user(r)
	writeJSON(w, http.StatusOK, configView{
		User:       user,
		ReadOnly:   s.opts.ReadOnly,
		CanWrite:   !s.opts.ReadOnly && user != "",
		UserHeader: s.opts.UserHeader,
		DefaultTTL: s.opts.DefaultTTL.Hours(),
		MaxTTL:     s.opts.MaxTTL.Hours(),
	})
}

func (s *Server) inScope(namespace string) bool {
	return s.opts.Namespace == "" || s.opts.Namespace == namespace
}

func (s *Server) handleList(w http.ResponseWriter, r *http.Request) {
	namespace := s.opts.Namespace
	if q := r.URL.Query().Get("namespace"); q != "" {
		if !s.inScope(q) {
			writeError(w, http.StatusForbidden, "namespace %q is out of scope of this page", q)
			return
		}
		namespace = q
	}
	list, err := s.vpas.AutoscalingV1().VerticalPodAutoscalers(namespace).List(r.Context(), metav1.ListOptions{})
	if err != nil {
		writeError(w, http.StatusBadGateway, "cannot list VPAs: %v", err)
		return
	}
	items := make([]vpaSummary, 0, len(list.Items))
	for i := range list.Items {
		items = append(items, s.summarize(&list.Items[i]))
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items})
}

func (s *Server) summarize(vpa *vpa_types.VerticalPodAutoscaler) vpaSummary {
	sum := vpaSummary{
		Namespace:   vpa.Namespace,
		Name:        vpa.Name,
		UpdateMode:  string(vpa_types.UpdateModeAuto),
		Recommender: "default",
	}
	if ref := vpa.Spec.TargetRef; ref != nil {
		sum.TargetRef = targetRef{Kind: ref.Kind, Name: ref.Name}
	}
	if p := vpa.Spec.UpdatePolicy; p != nil && p.UpdateMode != nil {
		sum.UpdateMode = string(*p.UpdateMode)
	}
	if len(vpa.Spec.Recommenders) > 0 && vpa.Spec.Recommenders[0] != nil {
		sum.Recommender = vpa.Spec.Recommenders[0].Name
	}

	ov, err := override.FromAnnotations(vpa.Annotations)
	if err != nil {
		sum.OverrideError = err.Error()
	}
	sum.Override = viewOverride(ov, s.now())

	if rec := vpa.Status.Recommendation; rec != nil {
		sum.Recommended = map[string]*resources{}
		for _, c := range rec.ContainerRecommendations {
			sum.Recommended[c.ContainerName] = fromList(c.Target)
		}
	}
	return sum
}

func (s *Server) getVPA(w http.ResponseWriter, r *http.Request) *vpa_types.VerticalPodAutoscaler {
	namespace, name := r.PathValue("namespace"), r.PathValue("name")
	if !s.inScope(namespace) {
		writeError(w, http.StatusForbidden, "namespace %q is out of scope of this page", namespace)
		return nil
	}
	vpa, err := s.vpas.AutoscalingV1().VerticalPodAutoscalers(namespace).Get(r.Context(), name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		writeError(w, http.StatusNotFound, "vpa %s/%s not found", namespace, name)
		return nil
	}
	if err != nil {
		writeError(w, http.StatusBadGateway, "cannot get vpa %s/%s: %v", namespace, name, err)
		return nil
	}
	return vpa
}

func (s *Server) handleDetail(w http.ResponseWriter, r *http.Request) {
	vpa := s.getVPA(w, r)
	if vpa == nil {
		return
	}
	writeJSON(w, http.StatusOK, s.detail(r.Context(), vpa))
}

// detail gathers everything about one VPA. What cannot be read is reported next to
// the thing it belongs to instead of failing the whole page: a VictoriaMetrics that is
// down must not hide the manifest and the pods.
func (s *Server) detail(ctx context.Context, vpa *vpa_types.VerticalPodAutoscaler) vpaDetail {
	d := vpaDetail{vpaSummary: s.summarize(vpa), Containers: []containerView{}}

	wl, wlErr := getWorkload(ctx, s.kube, vpa.Namespace, vpa.Spec.TargetRef)
	var live map[string][]livePods
	if wlErr != nil {
		d.WorkloadError = wlErr.Error()
	} else {
		if wl.helm != "" {
			d.Helm = &helmInfo{Release: wl.helm}
		}
		var err error
		if live, err = livePodResources(ctx, s.kube, vpa.Namespace, wl.selector); err != nil {
			klog.ErrorS(err, "Cannot list pods", "vpa", klog.KObj(vpa))
		}
	}

	recommended := map[string]vpa_types.RecommendedContainerResources{}
	if rec := vpa.Status.Recommendation; rec != nil {
		for _, c := range rec.ContainerRecommendations {
			recommended[c.ContainerName] = c
		}
	}

	var names []string
	initial := map[string]corev1.Container{}
	if wl != nil {
		for _, c := range wl.containers {
			names = append(names, c.Name)
			initial[c.Name] = c
		}
	} else {
		names = sortedKeys(recommended)
	}

	prefix := ""
	if vpa.Spec.TargetRef != nil {
		prefix = vpa.Spec.TargetRef.Name
	}
	queryCtx, cancel := context.WithTimeout(ctx, s.opts.QueryTimeout)
	defer cancel()
	computed, computedErr := s.calc.GetRecommendedPodResources(queryCtx, vpa.Namespace, prefix, names)

	var ov *override.Override
	if d.Override != nil {
		ov, _ = override.FromAnnotations(vpa.Annotations)
	}

	for _, name := range names {
		c := containerView{Name: name, Live: live[name]}
		if spec, ok := initial[name]; ok {
			c.Initial = &initialResources{Requests: fromList(spec.Resources.Requests), Limits: fromList(spec.Resources.Limits)}
			c.LimitRatio = limitRatio(spec.Resources.Requests, spec.Resources.Limits)
		}
		if rec, ok := recommended[name]; ok {
			c.VPA = &recommendation{Target: fromList(rec.Target), LowerBound: fromList(rec.LowerBound), UpperBound: fromList(rec.UpperBound)}
		}
		if computedErr != nil {
			c.ComputedError = computedErr.Error()
		} else if r, ok := computed[name]; ok {
			c.Computed = fromCalculated(r)
		}
		usage, err := s.calc.GetCurrentUsage(queryCtx, vpa.Namespace, prefix, name)
		if err != nil {
			c.UsageError = err.Error()
		} else {
			c.Usage = &usage
		}
		if ov != nil {
			if pinned, ok := d.Override.Resources[name]; ok && ov.Mode == override.ModePinned {
				c.Pinned = &pinned
			}
		}
		d.Containers = append(d.Containers, c)
	}
	return d
}

// writer checks who may write. It answers the request itself when nobody may.
func (s *Server) writer(w http.ResponseWriter, r *http.Request) (string, bool) {
	if s.opts.ReadOnly {
		writeError(w, http.StatusForbidden, "the page is read-only")
		return "", false
	}
	user := s.user(r)
	if user == "" {
		writeError(w, http.StatusForbidden, "cannot tell who you are: no %s header, so the change would have no author", s.opts.UserHeader)
		return "", false
	}
	return user, true
}

func (s *Server) handlePutOverride(w http.ResponseWriter, r *http.Request) {
	user, ok := s.writer(w, r)
	if !ok {
		return
	}
	vpa := s.getVPA(w, r)
	if vpa == nil {
		return
	}

	var req putOverrideRequest
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxRequestBodyBytes))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "bad request body: %v", err)
		return
	}

	now := s.now()
	ov, err := s.buildOverride(r.Context(), vpa, req, user, now)
	if err != nil {
		writeError(w, http.StatusBadRequest, "%v", err)
		return
	}
	annotations, err := ov.ToAnnotations()
	if err != nil {
		writeError(w, http.StatusBadRequest, "%v", err)
		return
	}

	previous, _ := override.FromAnnotations(vpa.Annotations)
	if err := s.patchAnnotations(r.Context(), vpa, annotations); err != nil {
		writeError(w, http.StatusBadGateway, "cannot update the vpa: %v", err)
		return
	}

	msg := fmt.Sprintf("%s set %s until %s", user, ov.Mode, ov.ExpiresAt.UTC().Format(time.RFC3339))
	if ov.Reason != "" {
		msg += ": " + ov.Reason
	}
	if previous != nil && !previous.Expired(now) {
		msg += fmt.Sprintf(" (replaces the %s set by %s)", previous.Mode, previous.SetBy)
	}
	s.recordEvent(r.Context(), vpa, "RecommendationOverrideSet", msg, now)
	klog.InfoS("Override set", "vpa", klog.KObj(vpa), "user", user, "mode", ov.Mode, "expiresAt", ov.ExpiresAt)

	writeJSON(w, http.StatusOK, map[string]any{"override": viewOverride(ov, now)})
}

func (s *Server) handleDeleteOverride(w http.ResponseWriter, r *http.Request) {
	user, ok := s.writer(w, r)
	if !ok {
		return
	}
	vpa := s.getVPA(w, r)
	if vpa == nil {
		return
	}

	previous, _ := override.FromAnnotations(vpa.Annotations)
	if err := s.patchAnnotations(r.Context(), vpa, nil); err != nil {
		writeError(w, http.StatusBadGateway, "cannot update the vpa: %v", err)
		return
	}

	msg := fmt.Sprintf("%s returned the VPA to Auto", user)
	if previous != nil {
		msg += fmt.Sprintf(" (was %s set by %s)", previous.Mode, previous.SetBy)
	}
	s.recordEvent(r.Context(), vpa, "RecommendationOverrideCleared", msg, s.now())
	klog.InfoS("Override cleared", "vpa", klog.KObj(vpa), "user", user)

	writeJSON(w, http.StatusOK, map[string]any{"override": nil})
}

// buildOverride turns the request into an override, rejecting what cannot be applied.
func (s *Server) buildOverride(ctx context.Context, vpa *vpa_types.VerticalPodAutoscaler, req putOverrideRequest, user string, now time.Time) (*override.Override, error) {
	ttl := time.Duration(req.TTLHours * float64(time.Hour))
	if req.TTLHours == 0 {
		ttl = s.opts.DefaultTTL
	}
	if ttl <= 0 {
		return nil, fmt.Errorf("ttlHours must be positive")
	}
	if s.opts.MaxTTL > 0 && ttl > s.opts.MaxTTL {
		return nil, fmt.Errorf("an override can last at most %s, asked for %s", s.opts.MaxTTL, ttl)
	}

	ov := &override.Override{
		Mode:      override.Mode(req.Mode),
		SetBy:     user,
		SetAt:     now,
		ExpiresAt: now.Add(ttl),
		Reason:    strings.TrimSpace(req.Reason),
	}

	switch ov.Mode {
	case override.ModePaused:
		if len(req.Containers) > 0 {
			return nil, fmt.Errorf("mode %s takes no containers", override.ModePaused)
		}
	case override.ModePinned:
		known, err := s.containerNames(ctx, vpa)
		if err != nil {
			return nil, err
		}
		if ov.Resources, err = parseQuantities(req.Containers, known); err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("mode must be %q or %q, got %q", override.ModePinned, override.ModePaused, req.Mode)
	}
	return ov, nil
}

// parseQuantities reads container -> {cpu, memory} as typed in the form, rejecting
// containers the VPA does not manage, resources other than cpu and memory, and text that
// is not a quantity. Whether the values are complete is for override.Validate.
func parseQuantities(containers map[string]map[string]string, known map[string]bool) (map[string]corev1.ResourceList, error) {
	out := make(map[string]corev1.ResourceList, len(containers))
	for _, name := range sortedKeys(containers) {
		if !known[name] {
			return nil, fmt.Errorf("the VPA has no container %q", name)
		}
		list := corev1.ResourceList{}
		for res, raw := range containers[name] {
			if res != string(corev1.ResourceCPU) && res != string(corev1.ResourceMemory) {
				return nil, fmt.Errorf("container %q: unsupported resource %q", name, res)
			}
			q, err := resource.ParseQuantity(strings.TrimSpace(raw))
			if err != nil {
				return nil, fmt.Errorf("container %q: %s %q is not a quantity", name, res, raw)
			}
			list[corev1.ResourceName(res)] = q
		}
		out[name] = list
	}
	return out, nil
}

// containerNames are the containers the VPA can manage: those of the workload, or, when
// it cannot be read, those the recommender already published.
func (s *Server) containerNames(ctx context.Context, vpa *vpa_types.VerticalPodAutoscaler) (map[string]bool, error) {
	known := map[string]bool{}
	if wl, err := getWorkload(ctx, s.kube, vpa.Namespace, vpa.Spec.TargetRef); err == nil {
		for _, c := range wl.containers {
			known[c.Name] = true
		}
		return known, nil
	}
	if rec := vpa.Status.Recommendation; rec != nil {
		for _, c := range rec.ContainerRecommendations {
			known[c.ContainerName] = true
		}
	}
	if len(known) == 0 {
		return nil, errors.New("cannot tell the containers of the VPA: its workload is unreadable and it has no recommendation yet")
	}
	return known, nil
}

// patchAnnotations makes the override annotations exactly annotations: the keys in it
// are set, every other override key is removed. A merge patch does both in one request.
func (s *Server) patchAnnotations(ctx context.Context, vpa *vpa_types.VerticalPodAutoscaler, annotations map[string]string) error {
	patch := map[string]any{}
	for _, key := range override.AnnotationKeys() {
		if v, ok := annotations[key]; ok {
			patch[key] = v
		} else {
			patch[key] = nil
		}
	}
	body, err := json.Marshal(map[string]any{"metadata": map[string]any{"annotations": patch}})
	if err != nil {
		return err
	}
	_, err = s.vpas.AutoscalingV1().VerticalPodAutoscalers(vpa.Namespace).Patch(ctx, vpa.Name, types.MergePatchType, body, metav1.PatchOptions{})
	return err
}

// recordEvent leaves a trail on the VPA. Removing an override deletes its annotations,
// so without an event nothing would say who did it. A failure here is logged, not
// returned: the annotations are the record that matters.
func (s *Server) recordEvent(ctx context.Context, vpa *vpa_types.VerticalPodAutoscaler, reason, message string, now time.Time) {
	stamp := metav1.NewTime(now)
	event := &corev1.Event{
		ObjectMeta: metav1.ObjectMeta{Namespace: vpa.Namespace, Name: fmt.Sprintf("%s.%x%x", truncate(vpa.Name, 200), now.UnixNano(), s.eventSeq.Add(1))},
		InvolvedObject: corev1.ObjectReference{
			APIVersion: vpa_types.SchemeGroupVersion.String(), Kind: "VerticalPodAutoscaler",
			Namespace: vpa.Namespace, Name: vpa.Name, UID: vpa.UID,
		},
		Reason: reason, Message: message, Type: corev1.EventTypeNormal,
		Source:         corev1.EventSource{Component: eventSource},
		FirstTimestamp: stamp, LastTimestamp: stamp, Count: 1,
	}
	if _, err := s.kube.CoreV1().Events(vpa.Namespace).Create(ctx, event, metav1.CreateOptions{}); err != nil {
		klog.ErrorS(err, "Cannot record event", "vpa", klog.KObj(vpa), "reason", reason)
	}
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
