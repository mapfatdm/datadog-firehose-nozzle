package cloudfoundry

import (
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("rlpGatewayClientDoer", func() {
	It("reuses the connection across requests", func() {
		var conns int32
		server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Write([]byte("ok")) //nolint:errcheck
		}))
		server.Config.ConnState = func(_ net.Conn, s http.ConnState) {
			if s == http.StateNew {
				atomic.AddInt32(&conns, 1)
			}
		}
		server.Start()
		defer server.Close()

		doer := newRLPGatewayClientDoer(true, nil, false, 0)
		for i := 0; i < 3; i++ {
			req, err := http.NewRequest(http.MethodGet, server.URL, nil)
			Expect(err).ToNot(HaveOccurred())
			resp, err := doer.Do(req)
			Expect(err).ToNot(HaveOccurred())
			io.Copy(io.Discard, resp.Body) //nolint:errcheck
			resp.Body.Close()
		}

		Expect(atomic.LoadInt32(&conns)).To(Equal(int32(1)))
	})
	Context("idle timeout", func() {
		var server *httptest.Server

		// streamingServer writes one chunk every interval, count times, and then
		// stays silent until the client goes away.
		streamingServer := func(interval time.Duration, count int) {
			server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				for i := 0; i < count; i++ {
					w.Write([]byte("x")) //nolint:errcheck
					w.(http.Flusher).Flush()
					time.Sleep(interval)
				}
				<-r.Context().Done()
			}))
		}

		doRead := func(idleTimeout time.Duration) io.ReadCloser {
			doer := newRLPGatewayClientDoer(true, nil, false, idleTimeout)
			req, err := http.NewRequest(http.MethodGet, server.URL, nil)
			Expect(err).ToNot(HaveOccurred())
			resp, err := doer.Do(req)
			Expect(err).ToNot(HaveOccurred())
			return resp.Body
		}

		AfterEach(func() {
			server.Close()
		})

		It("closes a stream that stays silent", func() {
			streamingServer(0, 1)
			body := doRead(200 * time.Millisecond)
			defer body.Close()

			done := make(chan error, 1)
			go func() {
				_, err := io.Copy(io.Discard, body)
				done <- err
			}()
			Eventually(done, 5*time.Second).Should(Receive(HaveOccurred()))
		})

		It("keeps a stream open while data keeps arriving", func() {
			streamingServer(100*time.Millisecond, 8)
			body := doRead(300 * time.Millisecond)
			defer body.Close()

			buf := make([]byte, 1)
			for i := 0; i < 8; i++ {
				_, err := body.Read(buf)
				Expect(err).ToNot(HaveOccurred())
			}
		})
	})
})
