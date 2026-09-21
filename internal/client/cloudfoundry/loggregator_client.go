package cloudfoundry

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"net/http"
	"regexp"
	"time"

	"github.com/DataDog/datadog-firehose-nozzle/internal/config"
	"github.com/DataDog/datadog-firehose-nozzle/internal/logger"

	"code.cloudfoundry.org/go-loggregator"
	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	"github.com/cloudfoundry/gosteno"
)

// AuthTokenFetcher is an interface for fetching an auth token from uaa
type AuthTokenFetcher interface {
	FetchAuthToken() string
}

type LoggregatorClient struct {
	RLPGatewayClient      *loggregator.RLPGatewayClient
	stopConsumer          context.CancelFunc
	shardId               string
	enableApplicationLogs bool
}

type rlpGatewayClientDoer struct {
	token        string
	disableACS   bool
	tokenFetcher AuthTokenFetcher
	client       *http.Client
}

func (d *rlpGatewayClientDoer) Do(req *http.Request) (*http.Response, error) {
	if !d.disableACS && d.token == "" {
		d.token = d.tokenFetcher.FetchAuthToken()
	}

	req.Close = true
	req.Header.Set("Authorization", d.token)
	resp, err := d.client.Do(req)
	if err != nil {
		return resp, err
	}

	// If token is bad, try to refresh token and retry request once but only if access
	// control is enabled
	if d.disableACS {
		return resp, err
	}

	if resp.StatusCode >= http.StatusBadRequest {
		time.Sleep(200 * time.Millisecond)
		d.token = d.tokenFetcher.FetchAuthToken()

		req.Header.Set("Authorization", d.token)
		resp, err = d.client.Do(req)
	}

	return resp, err
}

func newRLPGatewayClientDoer(disableACS bool, tokenFetcher AuthTokenFetcher, insecureSkipVerify bool) *rlpGatewayClientDoer {
	client := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: insecureSkipVerify,
			},
		},
		Timeout: 1 * time.Minute,
	}

	return &rlpGatewayClientDoer{
		token:        "",
		disableACS:   disableACS,
		tokenFetcher: tokenFetcher,
		client:       client,
	}
}

func NewLoggregatorClient(cfg *config.Config, lgr *gosteno.Logger, authTokenFetcher AuthTokenFetcher) (*LoggregatorClient, error) {
	var logStreamURL string
	if cfg.RLPGatewayURL != "" {
		logStreamURL = cfg.RLPGatewayURL
	} else if cfg.CloudControllerEndpoint != "" {
		re := regexp.MustCompile("://(api)")
		logStreamURL = re.ReplaceAllString(cfg.CloudControllerEndpoint, "://log-stream")
	} else {
		return nil, fmt.Errorf("neither CloudControllerEndpoint nor RLPGatewayURL specified, can't determine log stream URL")
	}

	logForwarder := *logger.NewRLPLogForwarder(lgr)
	return &LoggregatorClient{
		RLPGatewayClient: loggregator.NewRLPGatewayClient(
			logStreamURL,
			loggregator.WithRLPGatewayClientLogger(log.New(logForwarder, "", log.LstdFlags)),
			loggregator.WithRLPGatewayHTTPClient(
				newRLPGatewayClientDoer(cfg.DisableAccessControl, authTokenFetcher, cfg.InsecureSSLSkipVerify),
			),
		),
		shardId:               cfg.FirehoseSubscriptionID,
		stopConsumer:          nil,
		enableApplicationLogs: cfg.EnableApplicationLogs,
	}, nil
}

func (l *LoggregatorClient) EnvelopeStream() loggregator.EnvelopeStream {
	ctx := context.Background()
	ctx, l.stopConsumer = context.WithCancel(context.Background())

	selectors := []*loggregator_v2.Selector{
		{
			Message: &loggregator_v2.Selector_Counter{
				Counter: &loggregator_v2.CounterSelector{},
			},
		},
		{
			Message: &loggregator_v2.Selector_Gauge{
				Gauge: &loggregator_v2.GaugeSelector{},
			},
		},
	}
	// Only subscribe to the Log selector if application logs are actually
	// forwarded (see ProcessLog in internal/nozzle/workers.go). Otherwise the
	// nozzle asks the RLP gateway for a stream it discards anyway, crowding
	// out Counter/Gauge envelopes in the RLP's fixed per-consumer buffer.
	if l.enableApplicationLogs {
		selectors = append(selectors, &loggregator_v2.Selector{
			Message: &loggregator_v2.Selector_Log{
				Log: &loggregator_v2.LogSelector{},
			},
		})
	}

	es := l.RLPGatewayClient.Stream(
		ctx,
		&loggregator_v2.EgressBatchRequest{
			ShardId:   l.shardId,
			Selectors: selectors,
		},
	)

	return es
}

func (l *LoggregatorClient) Stop() {
	if l.stopConsumer != nil {
		l.stopConsumer()
	}
}
