package gokini

import (
	"errors"
	"os"
	"os/signal"
	"sort"
	"sync"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

const (
	defaultEmptyRecordBackoffMs = 500
	// ErrCodeKMSThrottlingException is defined in the API Reference https://docs.aws.amazon.com/sdk-for-go/api/service/kinesis/#Kinesis.GetRecords
	// But it's not a constant?
	ErrCodeKMSThrottlingException = "KMSThrottlingException"
)

// RecordConsumer is the interface consumers will implement
type RecordConsumer interface {
	Init(string) error
	ProcessRecords([]*Records, *KinesisConsumer)
	Shutdown()
}

// Records is structure for Kinesis Records
type Records struct {
	Data           []byte `json:"data"`
	PartitionKey   string `json:"partitionKey"`
	SequenceNumber string `json:"sequenceNumber"`
}

type shardStatus struct {
	ID           string
	Checkpoint   string
	AssignedTo   string
	mux          *sync.Mutex
	LeaseTimeout time.Time
}

// KinesisConsumer contains all the configuration and functions necessary to start the Kinesis Consumer
type KinesisConsumer struct {
	StreamName           string
	ShardIteratorType    string
	RecordConsumer       RecordConsumer
	TableName            string
	EmptyRecordBackoffMs int
	LeaseDuration        int
	Monitoring           MonitoringConfiguration
	svc                  kinesisiface.KinesisAPI
	checkpointer         Checkpointer
	stop                 *chan struct{}
	waitGroup            *sync.WaitGroup
	shardStatus          map[string]*shardStatus
	consumerID           string
	sigs                 *chan os.Signal
	mService             monitoringService
	eventLoopCounter     int
	getRecordsCounter    int
}

// StartConsumer starts the RecordConsumer, calls Init and starts sending records to ProcessRecords
func (kc *KinesisConsumer) StartConsumer() error {
	// Set Defaults
	if kc.EmptyRecordBackoffMs == 0 {
		kc.EmptyRecordBackoffMs = defaultEmptyRecordBackoffMs
	}

	kc.consumerID = uuid.New().String()

	err := kc.Monitoring.init(kc.StreamName, kc.consumerID)
	if err != nil {
		log.Errorf("Failed to start monitoring service: %s", err)
	}
	kc.mService = kc.Monitoring.service

	if kc.svc == nil && kc.checkpointer == nil {
		log.Debugf("Creating Kinesis Session")
		session, err := session.NewSessionWithOptions(
			session.Options{
				SharedConfigState: session.SharedConfigEnable,
			},
		)
		if err != nil {
			return err
		}
		if endpoint := os.Getenv("KINESIS_ENDPOINT"); endpoint != "" {
			session.Config.Endpoint = aws.String(endpoint)
		}
		kc.svc = kinesis.New(session)
		kc.checkpointer = &DynamoCheckpoint{
			TableName:     kc.TableName,
			Retries:       5,
			LeaseDuration: kc.LeaseDuration,
		}
	}

	log.Debugf("Initializing Checkpointer")
	if err := kc.checkpointer.Init(); err != nil {
		log.Fatalf("Failed to start Checkpointer: %s", err)
	}

	kc.shardStatus = make(map[string]*shardStatus)

	sigs := make(chan os.Signal, 1)
	kc.sigs = &sigs
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	stopChan := make(chan struct{})
	kc.stop = &stopChan

	wg := sync.WaitGroup{}
	kc.waitGroup = &wg

	err = kc.getShardIDs("")
	if err != nil {
		log.Errorf("Error getting Kinesis shards: %s", err)
		return err
	}
	go kc.eventLoop()

	return nil
}

func (kc *KinesisConsumer) eventLoop() {
	for {
		log.Debug("Getting shards")
		err := kc.getShardIDs("")
		if err != nil {
			log.Errorf("Error getting Kinesis shards: %s", err)
			// Back-off?
			time.Sleep(500 * time.Millisecond)
		}
		log.Debugf("Found %d shards", len(kc.shardStatus))

		for _, shard := range kc.shardStatus {
			err := kc.checkpointer.FetchCheckpoint(shard)
			if err != nil {
				if err != ErrSequenceIDNotFound {
					log.Fatal(err)
				}
			}

			// We already own this shard so carry on
			if shard.AssignedTo == kc.consumerID {
				continue
			}

			err = kc.checkpointer.GetLease(shard, kc.consumerID)
			if err != nil {
				if err.Error() == ErrLeaseNotAquired {
					continue
				}
				log.Fatal(err)
			}

			kc.startShardConsumer(shard)
		}

		kc.balanceShards()

		kc.eventLoopCounter++
		select {
		case sig := <-*kc.sigs:
			log.Infof("Received signal %s. Exiting", sig)
			kc.Shutdown()
			return
		case <-*kc.stop:
			log.Info("Shutting down")
			return
		case <-time.After(1 * time.Second):
		}
	}
}

func (kc *KinesisConsumer) startShardConsumer(shard *shardStatus) {
	kc.mService.leaseGained(shard.ID)
	kc.RecordConsumer.Init(shard.ID)
	log.Debugf("Starting consumer for shard %s on %s", shard.ID, shard.AssignedTo)
	go kc.getRecords(shard.ID)
	kc.waitGroup.Add(1)
}

// Shutdown stops consuming records gracefully
func (kc *KinesisConsumer) Shutdown() {
	close(*kc.stop)
	kc.waitGroup.Wait()
}

func (kc *KinesisConsumer) getShardIDs(startShardID string) error {
	args := &kinesis.DescribeStreamInput{
		StreamName: aws.String(kc.StreamName),
	}
	if startShardID != "" {
		args.ExclusiveStartShardId = aws.String(startShardID)
	}
	streamDesc, err := kc.svc.DescribeStream(args)
	if err != nil {
		return err
	}

	if *streamDesc.StreamDescription.StreamStatus != "ACTIVE" {
		return errors.New("Stream not active")
	}

	var lastShardID string
	for _, s := range streamDesc.StreamDescription.Shards {
		if _, ok := kc.shardStatus[*s.ShardId]; !ok {
			log.Debugf("Found shard with id %s", *s.ShardId)
			kc.shardStatus[*s.ShardId] = &shardStatus{
				ID:  *s.ShardId,
				mux: &sync.Mutex{},
			}
		}
		lastShardID = *s.ShardId
	}

	if *streamDesc.StreamDescription.HasMoreShards {
		err := kc.getShardIDs(lastShardID)
		if err != nil {
			return err
		}
	}

	return nil
}

func (kc *KinesisConsumer) getShardIterator(shard *shardStatus) (*string, error) {
	err := kc.checkpointer.FetchCheckpoint(shard)
	if err != nil && err != ErrSequenceIDNotFound {
		return nil, err
	}

	if shard.Checkpoint == "" {
		shardIterArgs := &kinesis.GetShardIteratorInput{
			ShardId:           &shard.ID,
			ShardIteratorType: &kc.ShardIteratorType,
			StreamName:        &kc.StreamName,
		}
		iterResp, err := kc.svc.GetShardIterator(shardIterArgs)
		if err != nil {
			return nil, err
		}
		return iterResp.ShardIterator, nil
	}

	shardIterArgs := &kinesis.GetShardIteratorInput{
		ShardId:                &shard.ID,
		ShardIteratorType:      aws.String("AFTER_SEQUENCE_NUMBER"),
		StartingSequenceNumber: &shard.Checkpoint,
		StreamName:             &kc.StreamName,
	}
	iterResp, err := kc.svc.GetShardIterator(shardIterArgs)
	if err != nil {
		return nil, err
	}
	return iterResp.ShardIterator, nil
}

func (kc *KinesisConsumer) getRecords(shardID string) {
	defer kc.waitGroup.Done()

	shard := kc.shardStatus[shardID]
	shardIterator, err := kc.getShardIterator(shard)
	if err != nil {
		log.Fatalf("Unable to get shard iterator for %s: %s", shardID, err)
	}

	var retriedErrors int

	for {
		getRecordsStartTime := time.Now()
		if time.Now().UTC().After(shard.LeaseTimeout.Add(-5 * time.Second)) {
			err = kc.checkpointer.GetLease(shard, kc.consumerID)
			if err != nil {
				if err.Error() == ErrLeaseNotAquired {
					shard.mux.Lock()
					defer shard.mux.Unlock()
					shard.AssignedTo = ""
					kc.mService.leaseLost(shard.ID)
					return
				}
				log.Fatal(err)
			}
		}

		getRecordsArgs := &kinesis.GetRecordsInput{
			ShardIterator: shardIterator,
		}
		getResp, err := kc.svc.GetRecords(getRecordsArgs)
		if err != nil {
			if awsErr, ok := err.(awserr.Error); ok {
				if awsErr.Code() == kinesis.ErrCodeProvisionedThroughputExceededException || awsErr.Code() == ErrCodeKMSThrottlingException {
					log.Errorf("Error getting records from shard %v: %v", shardID, err)
					retriedErrors++
					time.Sleep(time.Duration(2^retriedErrors*100) * time.Millisecond)
					continue
				}
			}
			log.Fatalf("Error getting records from Kinesis that cannot be retried: %s\nRequest: %s", err, getRecordsArgs)
		}
		retriedErrors = 0

		var records []*Records
		var recordBytes int64
		for _, r := range getResp.Records {
			record := &Records{
				Data:           r.Data,
				PartitionKey:   *r.PartitionKey,
				SequenceNumber: *r.SequenceNumber,
			}
			records = append(records, record)
			recordBytes += int64(len(record.Data))
		}
		processRecordsStartTime := time.Now()
		kc.RecordConsumer.ProcessRecords(records, kc)
		// Convert from nanoseconds to milliseconds
		processedRecordsTiming := time.Since(processRecordsStartTime) / 1000000
		kc.mService.recordProcessRecordsTime(shard.ID, float64(processedRecordsTiming))

		if len(records) == 0 {
			log.Debug("No Kinesis records retrieved, backing off")
			time.Sleep(time.Duration(kc.EmptyRecordBackoffMs) * time.Millisecond)
		} else {
			checkpoint := *getResp.Records[len(getResp.Records)-1].SequenceNumber
			shard.mux.Lock()
			shard.Checkpoint = checkpoint
			shard.mux.Unlock()
			kc.checkpointer.CheckpointSequence(shard)
		}

		kc.mService.incrRecordsProcessed(shard.ID, len(records))
		kc.mService.incrBytesProcessed(shard.ID, recordBytes)
		kc.mService.millisBehindLatest(shard.ID, float64(*getResp.MillisBehindLatest))

		// Convert from nanoseconds to milliseconds
		getRecordsTime := time.Since(getRecordsStartTime) / 1000000
		kc.mService.recordGetRecordsTime(shard.ID, float64(getRecordsTime))

		// The shard has been closed, so no new records can be read from it
		if getResp.NextShardIterator == nil {
			log.Debugf("Shard %s closed", shardID)
			delete(kc.shardStatus, shardID)
			kc.RecordConsumer.Shutdown()
			return
		}
		shardIterator = getResp.NextShardIterator

		select {
		case <-*kc.stop:
			kc.RecordConsumer.Shutdown()
			return
		case <-time.After(1 * time.Nanosecond):
		}
		kc.getRecordsCounter++
	}
}

type By func(p1, p2 *workerNode) bool

func (by By) Sort(workerNodes []*workerNode) {
	wns := &workerNodeSorter{
		workerNodes: workerNodes,
		by:          by,
	}
	sort.Sort(wns)
}

type workerNode struct {
	ID     string
	shards []*shardStatus
}

type workerNodeSorter struct {
	workerNodes []*workerNode
	by          func(p1, p2 *workerNode) bool
}

func (w *workerNodeSorter) Len() int {
	return len(w.workerNodes)
}

func (w *workerNodeSorter) Swap(i, j int) {
	w.workerNodes[i], w.workerNodes[j] = w.workerNodes[j], w.workerNodes[i]
}

func (w *workerNodeSorter) Less(i, j int) bool {
	return w.by(w.workerNodes[i], w.workerNodes[j])
}

func (kc *KinesisConsumer) balanceShards() {
	if len(kc.shardStatus) < 1 {
		log.Debugln("No shards found, so no balancing done")
		return
	}

	workerNodes := make(map[string]*workerNode)
	var ourNodeOwnsShards bool
	for _, shard := range kc.shardStatus {
		kc.checkpointer.FetchCheckpoint(shard)
		if wn, ok := workerNodes[shard.AssignedTo]; ok {
			wn.shards = append(wn.shards, shard)
		} else {
			workerNodes[shard.AssignedTo] = &workerNode{
				ID: shard.AssignedTo,
				shards: []*shardStatus{
					shard,
				},
			}
		}
		if shard.AssignedTo == kc.consumerID {
			ourNodeOwnsShards = true
		}
	}
	if !ourNodeOwnsShards {
		workerNodes[kc.consumerID] = &workerNode{
			ID:     kc.consumerID,
			shards: []*shardStatus{},
		}
	}

	// len() returns an integer. integers automatically round down, which is what we want.
	desiredShardCount := len(kc.shardStatus) / len(workerNodes)

	// If we have enough shards no need to rebalance
	if len(workerNodes[kc.consumerID].shards) >= desiredShardCount {
		log.Debugln("We have enough shards, not balancing")
		return
	}

	var sortedWorkerNodes []*workerNode
	for _, wn := range workerNodes {
		sortedWorkerNodes = append(sortedWorkerNodes, wn)
	}
	sortByNumberOfShards := func(p1, p2 *workerNode) bool {
		return len(p1.shards) > len(p2.shards)
	}
	By(sortByNumberOfShards).Sort(sortedWorkerNodes)

	// Stealing a shard from a node with 1 or 0 shards isn't going to be productive
	if len(sortedWorkerNodes[0].shards) < 2 {
		log.Debugln("No worker has more than 1 shard, not balancing")
		return
	}

	// Start with the node with the most shards
	shardToSteal := sortedWorkerNodes[0].shards[0]
	shardToSteal.AssignedTo = kc.consumerID
	log.Debugln("Node", kc.consumerID, "stealing shard", shardToSteal.ID)
	kc.checkpointer.StealLease(shardToSteal, kc.consumerID)
	kc.startShardConsumer(shardToSteal)
}
