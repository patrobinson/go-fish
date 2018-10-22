package main

import (
	"bytes"
	"net/http"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/service/cloudwatch"
	"github.com/aws/aws-sdk-go/service/cloudwatch/cloudwatchiface"
	"github.com/gorilla/mux"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

type mockCloudWatch struct {
	cloudwatchiface.CloudWatchAPI
	metricData []*cloudwatch.MetricDatum
}

func (m *mockCloudWatch) PutMetricData(input *cloudwatch.PutMetricDataInput) (*cloudwatch.PutMetricDataOutput, error) {
	m.metricData = append(m.metricData, input.MetricData...)
	return &cloudwatch.PutMetricDataOutput{}, nil
}

func TestCloudWatchMonitoring(t *testing.T) {
	mockCW := &mockCloudWatch{}
	cwService := &cloudWatchMonitoringService{
		Namespace:       "testCloudWatchMonitoring",
		ResolutionSec:   1,
		svc:             mockCW,
		pipelineMetrics: map[string]*cloudWatchMetrics{},
	}
	cwService.incrPipelines("foo")
	cwService.flush()
	if len(mockCW.metricData) < 1 {
		t.Fatal("Expected at least one metric to be sent to cloudwatch")
	}

	if *mockCW.metricData[0].Value != float64(1) {
		t.Errorf("Expected metric value to be 1.0, got %f", *mockCW.metricData[0].Value)
	}
}

func TestPrometheusMonitoring(t *testing.T) {
	mConfig := &monitoringConfiguration{
		MonitoringService: "prometheus",
		Prometheus: prometheusMonitoringService{
			Namespace: "TestPrometheusMonitoring",
		},
	}
	router := mux.NewRouter()
	httpServer := &http.Server{
		Addr:         "127.0.0.1:8001",
		WriteTimeout: time.Second * 15,
		ReadTimeout:  time.Second * 15,
		IdleTimeout:  time.Second * 60,
		Handler:      router,
	}
	go httpServer.ListenAndServe()
	mService, err := mConfig.init(router)
	if err != nil {
		t.Fatalf("Failed to init monitoring service %s", err)
	}
	mService.incrPipelines("pipeline")
	resp, err := http.Get("http://localhost:8001/metrics")
	if err != nil {
		t.Fatalf("Error reading metrics %s", err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("Error getting metrics %s", err)
	}
	gatherer := prometheus.DefaultGatherer
	expected := bytes.NewReader([]byte(`# HELP TestPrometheusMonitoringPipelines The number of pipelines configured
	# TYPE TestPrometheusMonitoringPipelines gauge
	TestPrometheusMonitoringPipelines{pipelineName="pipeline"} 1
`))
	err = testutil.GatherAndCompare(gatherer, expected, "TestPrometheusMonitoringPipelines")
	if err != nil {
		t.Error(err)
	}
}
