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
	"net/http"
	"time"

	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/recommender/input/victoriametrics"
)

// historyRanges are the periods a chart can show. A fixed list keeps the cost of a
// request bounded: an arbitrary range would let one page ask VictoriaMetrics for months.
var historyRanges = map[string]time.Duration{
	"1h":  time.Hour,
	"6h":  6 * time.Hour,
	"24h": 24 * time.Hour,
	"3d":  3 * 24 * time.Hour,
	"7d":  7 * 24 * time.Hour,
}

const (
	defaultHistoryRange = "6h"
	chartPoints         = 240
	minChartStep        = 30 * time.Second
)

// historyStep is the distance between samples: about chartPoints of them over the period,
// never finer than the scrape interval makes sense, in whole multiples of it.
func historyStep(period time.Duration) time.Duration {
	step := period / chartPoints
	if step < minChartStep {
		return minChartStep
	}
	if rest := step % minChartStep; rest != 0 {
		step += minChartStep - rest
	}
	return step
}

type containerHistory struct {
	Name    string                   `json:"name"`
	History *victoriametrics.History `json:"history,omitempty"`
	Error   string                   `json:"error,omitempty"`
}

type historyView struct {
	Range       string             `json:"range"`
	Start       int64              `json:"start"`
	End         int64              `json:"end"`
	StepSeconds int64              `json:"stepSeconds"`
	Containers  []containerHistory `json:"containers"`
}

func (s *Server) handleHistory(w http.ResponseWriter, r *http.Request) {
	name := r.URL.Query().Get("range")
	if name == "" {
		name = defaultHistoryRange
	}
	period, ok := historyRanges[name]
	if !ok {
		writeError(w, http.StatusBadRequest, "range %q is not one of 1h, 6h, 24h, 3d, 7d", name)
		return
	}
	vpa := s.getVPA(w, r)
	if vpa == nil {
		return
	}

	known, err := s.containerNames(r.Context(), vpa)
	if err != nil {
		writeError(w, http.StatusBadGateway, "%v", err)
		return
	}
	prefix := ""
	if vpa.Spec.TargetRef != nil {
		prefix = vpa.Spec.TargetRef.Name
	}

	end := s.now()
	start := end.Add(-period)
	step := historyStep(period)
	view := historyView{Range: name, Start: start.Unix(), End: end.Unix(), StepSeconds: int64(step.Seconds()), Containers: []containerHistory{}}

	for _, container := range sortedKeys(known) {
		ch := containerHistory{Name: container}
		h, err := s.calc.GetHistory(r.Context(), vpa.Namespace, prefix, container, start, end, step)
		if err != nil {
			ch.Error = err.Error()
		} else {
			ch.History = &h
		}
		view.Containers = append(view.Containers, ch)
	}
	writeJSON(w, http.StatusOK, view)
}
