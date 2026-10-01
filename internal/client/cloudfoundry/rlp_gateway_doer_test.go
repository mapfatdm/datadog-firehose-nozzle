package cloudfoundry

import (
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"

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

		doer := newRLPGatewayClientDoer(true, nil, false)
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
})
