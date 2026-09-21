package cloudfoundry

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/DataDog/datadog-firehose-nozzle/internal/config"
	"github.com/cloudfoundry/gosteno"
)

var _ = Describe("LoggregatorClient", func() {
	Context("EnvelopeStream selectors", func() {
		var queries chan url.Values
		var server *httptest.Server

		subscribedSelectors := func(enableApplicationLogs bool) url.Values {
			cfg := &config.Config{
				RLPGatewayURL:         server.URL,
				DisableAccessControl:  true,
				EnableApplicationLogs: enableApplicationLogs,
			}
			client, err := NewLoggregatorClient(cfg, gosteno.NewLogger("test"), nil)
			Expect(err).ToNot(HaveOccurred())

			client.EnvelopeStream()
			defer client.stopConsumer()

			var q url.Values
			Eventually(queries, 5*time.Second).Should(Receive(&q))
			return q
		}

		BeforeEach(func() {
			queries = make(chan url.Values, 1)
			server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				select {
				case queries <- r.URL.Query():
				default:
				}
			}))
		})

		AfterEach(func() {
			server.Close()
		})

		It("does not subscribe to logs when application logs are disabled", func() {
			q := subscribedSelectors(false)
			Expect(q).To(HaveKey("counter"))
			Expect(q).To(HaveKey("gauge"))
			Expect(q).ToNot(HaveKey("log"))
		})

		It("subscribes to logs when application logs are enabled", func() {
			q := subscribedSelectors(true)
			Expect(q).To(HaveKey("counter"))
			Expect(q).To(HaveKey("gauge"))
			Expect(q).To(HaveKey("log"))
		})
	})
})
