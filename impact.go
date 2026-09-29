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
	"fmt"
	"net/http"
	"sort"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/klog/v2"

	vpa_types "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/apis/autoscaling.k8s.io/v1"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/recommender/model"
	vpa_api_util "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/utils/vpa"
)

// How bad an effect is. Restart is the one the page has to warn about before the
// button is pressed.
const (
	levelRestart = "restart" // a pod or a container will be restarted
	levelWarn    = "warn"    // the change will not do what was asked
	levelInfo    = "info"    // nothing happens now, or something to know
	levelOK      = "ok"      // applied without a restart
)

type effect struct {
	Level string `json:"level"`
	// Code names the case, so the tests and the page do not depend on the wording.
	Code string `json:"code"`
	Text string `json:"text"`
}

type containerImpact struct {
	Name    string   `json:"name"`
	Effects []effect `json:"effects"`
}

type impactView struct {
	Mode string `json:"mode"`
	// Restarts is true when any pod or container will be restarted.
	Restarts   bool              `json:"restarts"`
	Containers []containerImpact `json:"containers"`
}

// corridor is the recommended range the updater compares a pod's request with.
type corridor struct {
	LowerCPU, UpperCPU, LowerMem, UpperMem int64
}

// change is one container getting new requests: what the pods have now and what they
// are going to get.
type change struct {
	UpdateMode vpa_types.UpdateMode
	// Template is the container of the workload: the requests and limits its manifest
	// declares (the limit follows their proportion) and its resizePolicy.
	Template corev1.Container
	// Live is what the running pods have now.
	Live []livePods
	// Next is what will be published as the recommendation.
	NextCPUMilli, NextMemoryBytes int64
	// Corridor is the published range. Nil for a pin, where lower = target = upper.
	Corridor *corridor
	// MemoryUsageBytes is the largest memory working set among the pods now; 0 if unknown.
	MemoryUsageBytes int64
}

func memText(b int64) string { return resource.NewQuantity(b, resource.BinarySI).String() }

func podsText(changing, total int) string {
	return fmt.Sprintf("%d из %d подов", changing, total)
}

// restartsContainer says whether resizing the resource restarts the container in place:
// resizePolicy RestartContainer. The default is NotRequired.
func restartsContainer(t corev1.Container, res corev1.ResourceName) bool {
	for _, p := range t.ResizePolicy {
		if p.ResourceName == res {
			return p.RestartPolicy == corev1.RestartContainer
		}
	}
	return false
}

// projectedLimits are the limits the admission controller and the updater will set for
// the new requests: the manifest's limit/request proportion applied to them, by the
// very function they use.
func (c change) projectedLimits() (cpuMilli, memoryBytes int64) {
	next := corev1.ResourceList{
		corev1.ResourceCPU:    *resource.NewMilliQuantity(c.NextCPUMilli, resource.DecimalSI),
		corev1.ResourceMemory: *resource.NewQuantity(c.NextMemoryBytes, resource.BinarySI),
	}
	limits, _ := vpa_api_util.GetProportionalLimit(c.Template.Resources.Limits, c.Template.Resources.Requests, next, corev1.ResourceList{})
	return limits.Cpu().MilliValue(), limits.Memory().Value()
}

// needsChange says whether the updater will pick the pods of this group up. For a pin the
// recommended range is the pinned value itself, so any difference qualifies; otherwise the
// pod is picked up when its request is outside the corridor.
func (c change) needsChange(g livePods) bool {
	var cpu, memory int64
	if g.Requests != nil {
		cpu, memory = g.Requests.CPUMilli, g.Requests.MemoryBytes
	}
	if c.Corridor == nil {
		return cpu != c.NextCPUMilli || memory != c.NextMemoryBytes
	}
	k := c.Corridor
	return cpu < k.LowerCPU || cpu > k.UpperCPU || memory < k.LowerMem || memory > k.UpperMem
}

// analyze says what happens to the pods of a container when the change is applied, by the
// rules of Kubernetes 1.33 and of the VPA updater:
//   - Off: nothing is applied. Initial: only pods created later get the values.
//   - Recreate (and the deprecated Auto): the updater evicts pods, one at a time.
//   - InPlace and InPlaceOrRecreate: the values are patched into the running pod. Kubernetes
//     forbids lowering a memory limit in place unless the container's resizePolicy is
//     RestartContainer; InPlaceOrRecreate then falls back to evicting the pod, InPlace has no
//     fallback and the change stays unapplied. RestartContainer restarts the container
//     inside its pod whenever that resource changes.
//   - A new memory limit below what the container uses now gets it killed by the OOM killer.
func analyze(c change) []effect {
	mode := c.UpdateMode
	if mode == "" {
		mode = vpa_types.UpdateModeAuto
	}
	switch mode {
	case vpa_types.UpdateModeOff:
		return []effect{{levelInfo, "off", "updateMode: Off — updater и admission-controller не применяют рекомендацию, поды не изменятся."}}
	case vpa_types.UpdateModeInitial:
		return []effect{{levelInfo, "initial", "updateMode: Initial — значения получат только поды, созданные позже. Работающие поды не перезапускаются."}}
	}

	if len(c.Live) == 0 {
		return []effect{{levelInfo, "no-pods", "Работающих подов не найдено, оценить последствия нельзя."}}
	}

	limitCPU, limitMemory := c.projectedLimits()
	var total, changing int
	var cpuChanges, memoryChanges, lowersMemoryLimit, raises bool
	var largestOldMemoryLimit int64
	for _, g := range c.Live {
		total += g.Pods
		if !c.needsChange(g) {
			continue
		}
		changing += g.Pods

		var requestCPU, requestMemory, oldLimitCPU, oldLimitMemory int64
		if g.Requests != nil {
			requestCPU, requestMemory = g.Requests.CPUMilli, g.Requests.MemoryBytes
		}
		if g.Limits != nil {
			oldLimitCPU, oldLimitMemory = g.Limits.CPUMilli, g.Limits.MemoryBytes
		}
		cpuChanges = cpuChanges || requestCPU != c.NextCPUMilli || (limitCPU > 0 && oldLimitCPU != limitCPU)
		memoryChanges = memoryChanges || requestMemory != c.NextMemoryBytes || (limitMemory > 0 && oldLimitMemory != limitMemory)
		raises = raises || c.NextCPUMilli > requestCPU || c.NextMemoryBytes > requestMemory
		if limitMemory > 0 && oldLimitMemory > limitMemory {
			lowersMemoryLimit = true
			largestOldMemoryLimit = max(largestOldMemoryLimit, oldLimitMemory)
		}
	}

	if changing == 0 {
		if c.Corridor != nil {
			return []effect{{levelOK, "in-corridor", "Текущие requests подов в коридоре рекомендации, updater их сейчас не тронет."}}
		}
		return []effect{{levelOK, "no-change", "Поды уже с этими значениями, ничего не изменится."}}
	}

	var out []effect
	applied := true
	switch mode {
	case vpa_types.UpdateModeRecreate, vpa_types.UpdateModeAuto: //nolint:staticcheck
		out = append(out, effect{levelRestart, "recreate", fmt.Sprintf("Updater пересоздаст %s, по одному: поды будут перезапущены.", podsText(changing, total))})

	case vpa_types.UpdateModeInPlace, vpa_types.UpdateModeInPlaceOrRecreate:
		if lowersMemoryLimit && !restartsContainer(c.Template, corev1.ResourceMemory) {
			why := fmt.Sprintf("Kubernetes не даёт уменьшить лимит памяти на месте (%s → %s), пока у контейнера не задан resizePolicy: RestartContainer для памяти.",
				memText(largestOldMemoryLimit), memText(limitMemory))
			if mode == vpa_types.UpdateModeInPlaceOrRecreate {
				out = append(out, effect{levelRestart, "in-place-blocked-recreate", fmt.Sprintf("%s Updater откатится к пересозданию: %s будут перезапущены, по одному.", why, podsText(changing, total))})
			} else {
				applied = false
				out = append(out, effect{levelWarn, "in-place-blocked", fmt.Sprintf("%s Режим InPlace не пересоздаёт поды, поэтому изменение не применится (Infeasible), поды останутся со старыми значениями.", why)})
			}
			break
		}
		var restarted []string
		if cpuChanges && restartsContainer(c.Template, corev1.ResourceCPU) {
			restarted = append(restarted, "CPU")
		}
		if memoryChanges && restartsContainer(c.Template, corev1.ResourceMemory) {
			restarted = append(restarted, "памяти")
		}
		if len(restarted) > 0 {
			out = append(out, effect{levelRestart, "restart-container", fmt.Sprintf("Контейнер перезапустится внутри пода (%s): у него resizePolicy: RestartContainer для %s.", podsText(changing, total), joinRu(restarted))})
		} else {
			out = append(out, effect{levelOK, "in-place", fmt.Sprintf("Применится на месте, без перезапуска (%s).", podsText(changing, total))})
		}
		if raises {
			out = append(out, effect{levelInfo, "capacity", "Если на узле не хватит свободных ресурсов, ресайз повиснет в Pending или Infeasible."})
		}
	}

	if applied && limitMemory > 0 && c.MemoryUsageBytes > limitMemory {
		out = append(out, effect{levelRestart, "oom", fmt.Sprintf("Новый лимит памяти %s ниже текущего потребления %s: контейнер будет убит по OOM и перезапущен.", memText(limitMemory), memText(c.MemoryUsageBytes))})
	}

	sort.SliceStable(out, func(i, j int) bool { return severity(out[i].Level) < severity(out[j].Level) })
	return out
}

// severity orders effects: what needs attention first, the reassuring ones next, the notes last.
func severity(level string) int {
	switch level {
	case levelRestart:
		return 0
	case levelWarn:
		return 1
	case levelOK:
		return 2
	}
	return 3
}

func joinRu(parts []string) string {
	if len(parts) == 2 {
		return parts[0] + " и " + parts[1]
	}
	return parts[0]
}

// impactRequest is the body of POST .../impact: what the page is about to do. Auto is
// returning to the recommender, which the override API expresses by deleting the override.
type impactRequest struct {
	Mode       string                       `json:"mode"`
	Containers map[string]map[string]string `json:"containers"`
}

// handleImpact tells what applying a change would do to the pods. It writes nothing.
func (s *Server) handleImpact(w http.ResponseWriter, r *http.Request) {
	vpa := s.getVPA(w, r)
	if vpa == nil {
		return
	}
	var req impactRequest
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxRequestBodyBytes))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "bad request body: %v", err)
		return
	}

	wl, err := getWorkload(r.Context(), s.kube, vpa.Namespace, vpa.Spec.TargetRef)
	if err != nil {
		writeError(w, http.StatusBadGateway, "cannot read the workload, so the effect on its pods cannot be told: %v", err)
		return
	}
	known := map[string]bool{}
	names := make([]string, 0, len(wl.containers))
	for _, c := range wl.containers {
		known[c.Name] = true
		names = append(names, c.Name)
	}

	var pinned map[string]corev1.ResourceList
	switch req.Mode {
	case "Pinned":
		if pinned, err = parseQuantities(req.Containers, known); err != nil {
			writeError(w, http.StatusBadRequest, "%v", err)
			return
		}
		for name, list := range pinned {
			if _, ok := list[corev1.ResourceCPU]; !ok {
				writeError(w, http.StatusBadRequest, "container %q: cpu and memory are both needed", name)
				return
			}
			if _, ok := list[corev1.ResourceMemory]; !ok {
				writeError(w, http.StatusBadRequest, "container %q: cpu and memory are both needed", name)
				return
			}
		}
	case "Paused", "Auto":
	default:
		writeError(w, http.StatusBadRequest, `mode must be "Pinned", "Paused" or "Auto", got %q`, req.Mode)
		return
	}

	live, err := livePodResources(r.Context(), s.kube, vpa.Namespace, wl.selector)
	if err != nil {
		klog.ErrorS(err, "Cannot list pods", "vpa", klog.KObj(vpa))
	}

	prefix := ""
	if vpa.Spec.TargetRef != nil {
		prefix = vpa.Spec.TargetRef.Name
	}
	queryCtx, cancel := context.WithTimeout(r.Context(), s.opts.QueryTimeout)
	defer cancel()

	updateMode := vpa_types.UpdateModeAuto
	if p := vpa.Spec.UpdatePolicy; p != nil && p.UpdateMode != nil {
		updateMode = *p.UpdateMode
	}

	// Whatever is not pinned goes back to the calculation, so it has to be told what the
	// recommender would publish for it.
	var calculated map[string]calculatedRecommendation
	var calculatedErr error
	if req.Mode != "Paused" && len(pinned) < len(names) {
		calculated, calculatedErr = s.calculated(queryCtx, vpa.Namespace, prefix, names)
	}

	view := impactView{Mode: req.Mode, Containers: []containerImpact{}}
	for _, tc := range wl.containers {
		ci := containerImpact{Name: tc.Name}
		ch := change{UpdateMode: updateMode, Template: tc, Live: live[tc.Name]}

		switch list, isPinned := pinned[tc.Name]; {
		case req.Mode == "Paused":
			ci.Effects = []effect{{levelInfo, "paused", "Заморозка не меняет поды: recommender перестаёт менять рекомендацию, всё остаётся как есть."}}
		case isPinned:
			ch.NextCPUMilli, ch.NextMemoryBytes = list.Cpu().MilliValue(), list.Memory().Value()
		case calculatedErr != nil:
			ci.Effects = []effect{{levelWarn, "no-calculation", fmt.Sprintf("Расчёт recommender'а недоступен, поэтому последствия предсказать нельзя: %v", calculatedErr)}}
		default:
			rec, ok := calculated[tc.Name]
			if !ok {
				ci.Effects = []effect{{levelInfo, "no-calculation", "Для контейнера нет расчёта, последствия предсказать нельзя."}}
				break
			}
			ch.NextCPUMilli, ch.NextMemoryBytes, ch.Corridor = rec.cpu, rec.memory, &rec.corridor
		}

		if ci.Effects == nil {
			if usage, err := s.calc.GetCurrentUsage(queryCtx, vpa.Namespace, prefix, tc.Name); err == nil {
				ch.MemoryUsageBytes = int64(usage.MemoryBytesMax)
			}
			ci.Effects = analyze(ch)
		}
		for _, e := range ci.Effects {
			view.Restarts = view.Restarts || e.Level == levelRestart
		}
		view.Containers = append(view.Containers, ci)
	}
	writeJSON(w, http.StatusOK, view)
}

// calculatedRecommendation is what the recommender would publish for a container.
type calculatedRecommendation struct {
	cpu, memory int64
	corridor    corridor
}

func (s *Server) calculated(ctx context.Context, namespace, prefix string, names []string) (map[string]calculatedRecommendation, error) {
	recs, err := s.calc.GetRecommendedPodResources(ctx, namespace, prefix, names)
	if err != nil {
		return nil, err
	}
	out := make(map[string]calculatedRecommendation, len(recs))
	for name, r := range recs {
		out[name] = calculatedRecommendation{
			cpu:    int64(r.Target[model.ResourceCPU]),
			memory: int64(r.Target[model.ResourceMemory]),
			corridor: corridor{
				LowerCPU: int64(r.LowerBound[model.ResourceCPU]), UpperCPU: int64(r.UpperBound[model.ResourceCPU]),
				LowerMem: int64(r.LowerBound[model.ResourceMemory]), UpperMem: int64(r.UpperBound[model.ResourceMemory]),
			},
		}
	}
	return out, nil
}
