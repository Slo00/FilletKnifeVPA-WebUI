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

// Command ui is the web page for VPA recommendations: it shows what the workload
// declares, what the VPA recommends and what is consumed, and lets a person pin their
// own values or return the service to the recommender. See docs/adr/0003-manual-overrides.md.
package main

import (
	"context"
	"errors"
	"flag"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"

	"k8s.io/autoscaler/vertical-pod-autoscaler/common"
	vpa_clientset "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/client/clientset/versioned"
	recommender_config "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/recommender/config"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/recommender/input/victoriametrics"
)

func main() {
	// The page takes the recommender's flags as they are (VictoriaMetrics address, labels,
	// percentiles, margin): started with the same ones, it shows the numbers the
	// recommender calculates. Its own flags go first, InitRecommenderFlags parses them all.
	listen := flag.String("listen", ":8080", "Address the page listens on.")
	readOnly := flag.Bool("read-only", false, "Turn every write off.")
	userHeader := flag.String("user-header", "X-Forwarded-User", "Request header the SSO proxy in front of the page puts the user name in. Who set an override is taken from it, so the page must only be reachable through that proxy.")
	devUser := flag.String("dev-user", "", "User name to use when the header is absent. For local runs only.")
	defaultTTL := flag.Duration("default-override-ttl", 7*24*time.Hour, "How long an override lasts when the request does not say.")
	maxTTL := flag.Duration("max-override-ttl", 30*24*time.Hour, "The longest an override may last; 0 for no limit.")
	requestsMetric := flag.String("chart-requests-metric", victoriametrics.DefaultRequestsMetric, "Series with the requests containers declare, for the charts. kube-state-metrics has to be scraped into the same VictoriaMetrics.")
	limitsMetric := flag.String("chart-limits-metric", victoriametrics.DefaultLimitsMetric, "Series with the limits containers declare, for the charts.")
	queryTimeout := flag.Duration("view-query-timeout", 15*time.Second, "How long the VictoriaMetrics queries behind one page may take.")

	config := recommender_config.InitRecommenderFlags()

	kubeConfig := common.CreateKubeConfigOrDie(config.CommonFlags.KubeConfig, float32(config.CommonFlags.KubeApiQps), int(config.CommonFlags.KubeApiBurst))
	calc, err := victoriametrics.NewRecommenderFromConfig(config, victoriametrics.WithResourceMetrics(*requestsMetric, *limitsMetric))
	if err != nil {
		klog.ErrorS(err, "Cannot create the VictoriaMetrics client")
		klog.FlushAndExit(klog.ExitFlushTimeout, 1)
	}

	server := NewServer(kubernetes.NewForConfigOrDie(kubeConfig), vpa_clientset.NewForConfigOrDie(kubeConfig), calc, Options{
		Namespace:    config.CommonFlags.VpaObjectNamespace,
		ReadOnly:     *readOnly,
		UserHeader:   *userHeader,
		DevUser:      *devUser,
		DefaultTTL:   *defaultTTL,
		MaxTTL:       *maxTTL,
		QueryTimeout: *queryTimeout,
	})
	if *devUser != "" {
		klog.InfoS("Running with --dev-user: anyone can write as that user. Not for production.", "user", *devUser)
	}

	httpServer := &http.Server{
		Addr:              *listen,
		Handler:           server.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
		WriteTimeout:      2 * *queryTimeout,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = httpServer.Shutdown(shutdownCtx)
	}()

	klog.InfoS("Serving", "address", *listen, "namespace", config.CommonFlags.VpaObjectNamespace, "readOnly", *readOnly, "victoriaMetrics", config.PrometheusAddress)
	if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		klog.ErrorS(err, "Server stopped")
		klog.FlushAndExit(klog.ExitFlushTimeout, 1)
	}
}
