package main

import (
	"fmt"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/cloudwatch"
	"github.com/aws/aws-sdk-go/service/cloudwatch/cloudwatchiface"
	"github.com/gorilla/mux"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	log "github.com/sirupsen/logrus"
)

const defaultNamespace = "go-fish"

// MonitoringConfiguration allows you to configure how record processing metrics are exposed
type monitoringConfiguration struct {
	MonitoringService string // Type of monitoring to expose. Supported types are "prometheus" and "cloudwatch"
	Prometheus        prometheusMonitoringService
	CloudWatch        cloudWatchMonitoringService
}

type monitoringService interface {
	init(*mux.Router) error
	incrPipelines(string)
}

func (m *monitoringConfiguration) init(r *mux.Router) (monitoringService, error) {
	var service monitoringService
	switch m.MonitoringService {
	case "prometheus":
		service = &m.Prometheus
	case "cloudwatch":
		service = &m.CloudWatch
	case "":
		service = &noopMonitoringService{}
	default:
		return service, fmt.Errorf("Invalid monitoring service type %s", m.MonitoringService)
	}
	return service, service.init(r)
}

type noopMonitoringService struct{}

func (n *noopMonitoringService) init(_ *mux.Router) error { return nil }
func (n *noopMonitoringService) incrPipelines(string)     {}

type prometheusMonitoringService struct {
	Namespace string
	pipelines *prometheus.GaugeVec
}

func (p *prometheusMonitoringService) init(r *mux.Router) error {
	if p.Namespace == "" {
		p.Namespace = defaultNamespace
	}

	p.pipelines = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: p.Namespace + `_pipelines`,
		Help: "The number of pipelines configured",
	}, []string{"pipelineName"})

	metrics := []prometheus.Collector{
		p.pipelines,
	}
	for _, metric := range metrics {
		err := prometheus.Register(metric)
		if err != nil {
			return err
		}
	}

	r.Handle("/metrics", promhttp.Handler())
	return nil
}

func (p *prometheusMonitoringService) incrPipelines(pipelineName string) {
	p.pipelines.With(prometheus.Labels{"pipelineName": pipelineName}).Add(float64(1))
}

type cloudWatchMonitoringService struct {
	Namespace string
	// What granularity we should send metrics to CW at. Note setting this to 1 will cost quite a bit of money
	// At the time of writing (March 2018) about US$200 per month
	ResolutionSec   int
	svc             cloudwatchiface.CloudWatchAPI
	pipelineMetrics map[string]*cloudWatchMetrics
}

type cloudWatchMetrics struct {
	pipelines float64
	sync.Mutex
}

func (cw *cloudWatchMonitoringService) init(_ *mux.Router) error {
	if cw.Namespace == "" {
		cw.Namespace = defaultNamespace
	}

	if cw.ResolutionSec == 0 {
		cw.ResolutionSec = 60
	}

	session, err := session.NewSessionWithOptions(
		session.Options{
			SharedConfigState: session.SharedConfigEnable,
		},
	)
	if err != nil {
		return err
	}

	cw.svc = cloudwatch.New(session)
	return nil
}

func (cw *cloudWatchMonitoringService) flushDaemon() {
	previousFlushTime := time.Now()
	resolutionDuration := time.Duration(cw.ResolutionSec) * time.Second
	for {
		time.Sleep(resolutionDuration - time.Now().Sub(previousFlushTime))
		cw.flush()
		previousFlushTime = time.Now()
	}
}

func (cw *cloudWatchMonitoringService) flush() {
	for pipeline, metric := range cw.pipelineMetrics {
		metric.Lock()
		metricTimestamp := time.Now()
		_, err := cw.svc.PutMetricData(&cloudwatch.PutMetricDataInput{
			Namespace: aws.String(cw.Namespace),
			MetricData: []*cloudwatch.MetricDatum{
				&cloudwatch.MetricDatum{
					Dimensions: []*cloudwatch.Dimension{
						{
							Name:  aws.String("Pipeline"),
							Value: &pipeline,
						},
					},
					MetricName: aws.String("Pipelines"),
					Unit:       aws.String("Count"),
					Timestamp:  &metricTimestamp,
					Value:      aws.Float64(metric.pipelines),
				},
			},
		})
		metric.Unlock()
		if err != nil {
			log.Errorln("Error sending logs to CloudWatch", err)
		}
	}
}

func (cw *cloudWatchMonitoringService) incrPipelines(pipelineName string) {
	if _, ok := cw.pipelineMetrics["pipelines"]; !ok {
		cw.pipelineMetrics[pipelineName] = &cloudWatchMetrics{}
	}
	cw.pipelineMetrics[pipelineName].Lock()
	defer cw.pipelineMetrics[pipelineName].Unlock()
	cw.pipelineMetrics[pipelineName].pipelines += float64(1)
}
