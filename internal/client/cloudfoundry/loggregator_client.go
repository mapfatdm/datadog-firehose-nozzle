package cloudfoundry

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
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
	RLPGatewayClient *loggregator.RLPGatewayClient
	stopConsumer     context.CancelFunc
	shardId          string
}

type rlpGatewayClientDoer struct {
	token        string
	disableACS   bool
	tokenFetcher AuthTokenFetcher
	client       *http.Client
	idleTimeout  time.Duration
}

func (d *rlpGatewayClientDoer) Do(req *http.Request) (*http.Response, error) {
	if !d.disableACS && d.token == "" {
		d.token = d.tokenFetcher.FetchAuthToken()
	}

	req.Header.Set("Authorization", d.token)
	resp, err := d.client.Do(req)
	if err != nil {
		return resp, err
	}

	// If token is bad, try to refresh token and retry request once but only if access
	// control is enabled
	if d.disableACS {
		return d.wrapIdleTimeout(resp), err
	}

	if resp.StatusCode >= http.StatusBadRequest {
		// drain so the transport can put this connection back in the pool
		io.Copy(io.Discard, resp.Body) //nolint:errcheck
		resp.Body.Close()

		time.Sleep(200 * time.Millisecond)
		d.token = d.tokenFetcher.FetchAuthToken()

		req.Header.Set("Authorization", d.token)
		resp, err = d.client.Do(req)
	}

	return d.wrapIdleTimeout(resp), err
}

// wrapIdleTimeout closes the stream's body if no data arrives for the
// configured idle timeout, so the caller's read loop errors out and
// reconnects the same way it would on any other connection failure.
// http.Client.Timeout can't express this since it bounds the whole
// request, but /v2/read is meant to be read indefinitely.
func (d *rlpGatewayClientDoer) wrapIdleTimeout(resp *http.Response) *http.Response {
	if resp == nil || resp.Body == nil || d.idleTimeout <= 0 {
		return resp
	}
	resp.Body = newIdleTimeoutReadCloser(resp.Body, d.idleTimeout)
	return resp
}

type idleTimeoutReadCloser struct {
	io.ReadCloser
	timeout time.Duration
	timer   *time.Timer
}

func newIdleTimeoutReadCloser(rc io.ReadCloser, timeout time.Duration) *idleTimeoutReadCloser {
	return &idleTimeoutReadCloser{
		ReadCloser: rc,
		timeout:    timeout,
		timer:      time.AfterFunc(timeout, func() { rc.Close() }),
	}
}

func (irc *idleTimeoutReadCloser) Read(p []byte) (int, error) {
	n, err := irc.ReadCloser.Read(p)
	if err == nil {
		irc.timer.Reset(irc.timeout)
	}
	return n, err
}

func (irc *idleTimeoutReadCloser) Close() error {
	irc.timer.Stop()
	return irc.ReadCloser.Close()
}

func newRLPGatewayClientDoer(disableACS bool, tokenFetcher AuthTokenFetcher, insecureSkipVerify bool, idleTimeout time.Duration) *rlpGatewayClientDoer {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil // internal CF traffic, don't honor HTTP_PROXY/HTTPS_PROXY
	transport.TLSClientConfig = &tls.Config{
		InsecureSkipVerify: insecureSkipVerify,
	}
	transport.ResponseHeaderTimeout = 30 * time.Second

	client := &http.Client{
		Transport: transport,
	}

	return &rlpGatewayClientDoer{
		token:        "",
		disableACS:   disableACS,
		tokenFetcher: tokenFetcher,
		client:       client,
		idleTimeout:  idleTimeout,
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

	idleTimeout := time.Duration(cfg.IdleTimeoutSeconds) * time.Second

	logForwarder := *logger.NewRLPLogForwarder(lgr)
	return &LoggregatorClient{
		RLPGatewayClient: loggregator.NewRLPGatewayClient(
			logStreamURL,
			loggregator.WithRLPGatewayClientLogger(log.New(logForwarder, "", log.LstdFlags)),
			loggregator.WithRLPGatewayHTTPClient(
				newRLPGatewayClientDoer(cfg.DisableAccessControl, authTokenFetcher, cfg.InsecureSSLSkipVerify, idleTimeout),
			),
		),
		shardId:      cfg.FirehoseSubscriptionID,
		stopConsumer: nil,
	}, nil
}

func (l *LoggregatorClient) EnvelopeStream() loggregator.EnvelopeStream {
	ctx := context.Background()
	ctx, l.stopConsumer = context.WithCancel(context.Background())
	es := l.RLPGatewayClient.Stream(
		ctx,
		&loggregator_v2.EgressBatchRequest{
			ShardId: l.shardId,
			Selectors: []*loggregator_v2.Selector{
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
				{
					Message: &loggregator_v2.Selector_Log{
						Log: &loggregator_v2.LogSelector{},
					},
				},
			},
		},
	)

	return es
}

func (l *LoggregatorClient) Stop() {
	if l.stopConsumer != nil {
		l.stopConsumer()
	}
}
