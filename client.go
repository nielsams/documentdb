package documentdb

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"time"
)

type Clienter interface {
	Read(link string, ret interface{}, opts ...CallOption) (*Response, error)
	Delete(link string, opts ...CallOption) (*Response, error)
	Query(link string, query *Query, ret interface{}, opts ...CallOption) (*Response, error)
	Create(link string, body, ret interface{}, opts ...CallOption) (*Response, error)
	Upsert(link string, body, ret interface{}, opts ...CallOption) (*Response, error)
	Replace(link string, body, ret interface{}, opts ...CallOption) (*Response, error)
	Execute(link string, body, ret interface{}, opts ...CallOption) (*Response, error)
}

var testCount int

type Client struct {
	Config *Config
	http.Client

	DefaultEndpoint *CosmosEndpoint
	ReadLocations   []CosmosEndpoint
	WriteLocations  []CosmosEndpoint
}

func (c *Client) apply(r *Request, opts []CallOption) (err error) {
	if err = r.DefaultHeaders(c.Config.MasterKey); err != nil {
		return err
	}

	for i := 0; i < len(opts); i++ {
		if err = opts[i](r); err != nil {
			return err
		}
	}
	return nil
}

// Read resource by self link
func (c *Client) Read(link string, ret interface{}, opts ...CallOption) (*Response, error) {
	buf := buffers.Get().(*bytes.Buffer)
	buf.Reset()
	res, err := c.method(http.MethodGet, link, expectStatusCode(http.StatusOK), ret, buf, opts...)

	buffers.Put(buf)

	return res, err
}

// Delete resource by self link
func (c *Client) Delete(link string, opts ...CallOption) (*Response, error) {
	return c.method(http.MethodDelete, link, expectStatusCode(http.StatusNoContent), nil, &bytes.Buffer{}, opts...)
}

// Query resource
func (c *Client) Query(link string, query *Query, ret interface{}, opts ...CallOption) (*Response, error) {
	var (
		err error
		req *http.Request
		buf = buffers.Get().(*bytes.Buffer)
	)
	buf.Reset()
	defer buffers.Put(buf)

	if err = Serialization.EncoderFactory(buf).Encode(query); err != nil {
		return nil, err

	}
	endpoint := c.getCosmosEndpoint(EndpointType_ReadOnly)
	queryURL := endpoint.EndpointURL + "/" + link
	req, err = http.NewRequest(http.MethodPost, queryURL, buf)
	if err != nil {
		return nil, err
	}
	r := ResourceRequest(link, req)

	if err = c.apply(r, opts); err != nil {
		return nil, err
	}

	r.QueryHeaders(buf.Len())
	return c.do(r, expectStatusCode(http.StatusOK), ret, endpoint)
}

// Create resource
func (c *Client) Create(link string, body, ret interface{}, opts ...CallOption) (*Response, error) {
	data, err := stringify(body)
	if err != nil {
		return nil, err
	}
	buf := bytes.NewBuffer(data)
	return c.method(http.MethodPost, link, expectStatusCode(http.StatusCreated), ret, buf, opts...)
}

// Upsert resource
func (c *Client) Upsert(link string, body, ret interface{}, opts ...CallOption) (*Response, error) {
	opts = append(opts, Upsert())
	data, err := stringify(body)
	if err != nil {
		return nil, err
	}
	buf := bytes.NewBuffer(data)
	return c.method(http.MethodPost, link, expectStatusCodeXX(http.StatusOK), ret, buf, opts...)
}

// Replace resource
func (c *Client) Replace(link string, body, ret interface{}, opts ...CallOption) (*Response, error) {
	data, err := stringify(body)
	if err != nil {
		return nil, err
	}
	buf := bytes.NewBuffer(data)
	return c.method(http.MethodPut, link, expectStatusCode(http.StatusOK), ret, buf, opts...)
}

// Replace resource
// TODO: DRY, move to methods instead of actions(POST, PUT, ...)
func (c *Client) Execute(link string, body, ret interface{}, opts ...CallOption) (*Response, error) {
	data, err := stringify(body)
	if err != nil {
		return nil, err
	}
	buf := bytes.NewBuffer(data)
	return c.method(http.MethodPost, link, expectStatusCode(http.StatusOK), ret, buf, opts...)
}

// Private generic method resource
func (c *Client) method(method string, link string, validator statusCodeValidatorFunc, ret interface{}, body *bytes.Buffer, opts ...CallOption) (*Response, error) {
	var queryURL string
	var endpoint *CosmosEndpoint

	// With a GET request we only need the read endpoint. For others we get a readwrite endpoint
	// Note that for 'Query', the queryurl is set elsewhere.
	switch method {
	case http.MethodGet:
		endpoint = c.getCosmosEndpoint(EndpointType_ReadOnly)
		queryURL = endpoint.EndpointURL + "/" + link
	default:
		endpoint = c.getCosmosEndpoint(EndpointType_ReadWrite)
		queryURL = endpoint.EndpointURL + "/" + link
	}

	req, err := http.NewRequest(method, queryURL, body)
	if err != nil {
		return nil, err
	}

	r := ResourceRequest(link, req)

	if err = c.apply(r, opts); err != nil {
		return nil, err
	}

	return c.do(r, validator, ret, endpoint)

}

// Private Do function, DRY
func (c *Client) do(r *Request, validator statusCodeValidatorFunc, data interface{}, endpoint *CosmosEndpoint) (*Response, error) {
	var (
		resp               *http.Response
		err                error
		currentAttempt     int = 0
		responseStatusCode int = 0
	)
	for {
		currentAttempt++
		fmt.Printf("Attempt %d outgoing request to %s\n", currentAttempt, r.Request.URL)

		resp, err = c.Do(r.Request)

		// Happy path response:
		if err == nil && validator(resp.StatusCode) {
			if data == nil {
				return nil, nil
			}
			return &Response{resp.Header}, readJson(resp.Body, data)
		}

		if resp != nil {
			responseStatusCode = resp.StatusCode
			// There was a response, but not the statuscode that was expected
			if !validator(resp.StatusCode) {
				err = &RequestError{}
				readJson(resp.Body, &err)
			}
			resp.Body.Close()
		}

		// If there are no more retries, break out of the loop and return to caller
		if !c.shouldRetry(responseStatusCode, &currentAttempt, endpoint) {
			break
		}
	}

	// This is the fall through case where the request was not successful and we won't retry (anymore)
	return nil, err
}

// Read json response to given interface(struct, map, ..)
func readJson(reader io.Reader, data interface{}) error {
	return Serialization.DecoderFactory(reader).Decode(&data)
}

// Stringify body data
func stringify(body interface{}) (bt []byte, err error) {
	switch t := body.(type) {
	case string:
		bt = []byte(t)
	case []byte:
		bt = t
	default:
		bt, err = Serialization.Marshal(t)
	}
	return
}

func (c *Client) shouldRetry(statusCode int, currentAttempt *int, currentEndpoint *CosmosEndpoint) bool {
	// CosmosDB has many different status codes as input for the retry decision.
	// See https://docs.microsoft.com/en-us/rest/api/cosmos-db/http-status-codes-for-cosmosdb for details

	// We don't retry 400 - 413. They are unlikely to change on the next attempt and indicate an invalid request
	if statusCode >= 400 && statusCode <= 413 {
		return false
	}

	// For other errors, we will retry until the set retryCount
	if *currentAttempt < c.Config.RetryOptions.RetryCount {
		return true
	}

	// We've reached the retryCount. Marking region unhealthy and not retrying there for now.
	if *currentAttempt == c.Config.RetryOptions.RetryCount && !currentEndpoint.IsDefaultEndpoint {
		fmt.Printf("Connecting to endpoint %s failed, marking it as unavailable.\n", currentEndpoint.EndpointName)
		c.markEndpointUnavailable(currentEndpoint)
	}

	return false
}

// GetRegionalEndpoints sets the list of preferred read and write locations.
// It is called from documentDb.New() if the EnableEndpointDiscovery is set to true
func (c *Client) GetRegionalEndpoints() error {
	var (
		err                  error
		req                  *http.Request
		buf                  = buffers.Get().(*bytes.Buffer)
		endpointResponseBody EndpointDescription
	)
	buf.Reset()
	defer buffers.Put(buf)

	req, err = http.NewRequest(http.MethodGet, c.DefaultEndpoint.EndpointURL+"/", buf)
	if err != nil {
		return err
	}
	r := ResourceRequest("", req)

	if err = c.apply(r, []CallOption{}); err != nil {
		return err
	}
	r.QueryHeaders(buf.Len())

	_, err = c.do(r, expectStatusCode(http.StatusOK), &endpointResponseBody, c.DefaultEndpoint)
	if err != nil {
		return err
	}

	// We set the received region list to the definitive region list as a whole, in case no changes are made for PreferredLocation
	c.ReadLocations = endpointResponseBody.ReadableLocations
	c.WriteLocations = endpointResponseBody.WritableLocations

	// We have a preferred region. This region should be moved to the front of the list if it is in the list
	prefLoc := c.Config.RegionalOptions.PreferredLocation

	// For both read and write, we look in the list for the PreferredLocation. If we find it, we move it to the front of the list
	for i, loc := range endpointResponseBody.ReadableLocations {
		if loc.EndpointName == prefLoc {
			c.ReadLocations = nil
			c.ReadLocations = append(c.ReadLocations, endpointResponseBody.ReadableLocations[i])
			c.ReadLocations = append(c.ReadLocations, endpointResponseBody.ReadableLocations[:i]...)
			c.ReadLocations = append(c.ReadLocations, endpointResponseBody.ReadableLocations[i+1:]...)
		}
	}
	for i, loc := range endpointResponseBody.WritableLocations {
		if loc.EndpointName == prefLoc {
			c.WriteLocations = nil
			c.WriteLocations = append(c.WriteLocations, endpointResponseBody.WritableLocations[i])
			c.WriteLocations = append(c.WriteLocations, endpointResponseBody.WritableLocations[:i]...)
			c.WriteLocations = append(c.WriteLocations, endpointResponseBody.WritableLocations[i+1:]...)
		}
	}

	return nil
}

// getCosmosEndpoint returns the endpoint that will be used in the ongoing call.
// By specifying either EndpointType_ReadOnly or EndpointType_ReadWrite, we select the top endpoint from that list that is not unavailable.
// If no multiregion config/setup is used, the ReadLocations and WriteLocations lists are empty and the default c.DefaultEndpoint will be returned
func (c *Client) getCosmosEndpoint(endpointType EndpointType) *CosmosEndpoint {

	// First we need to mark endpoints available again if needed
	c.purgeStaleEndpointUnavailability()

	switch endpointType {

	case EndpointType_ReadOnly:
		for i, endpoint := range c.ReadLocations {
			if !endpoint.IsUnavailable {
				return &c.ReadLocations[i]
			}
		}

	case EndpointType_ReadWrite:
		for i, endpoint := range c.WriteLocations {
			if !endpoint.IsUnavailable && c.Config.RegionalOptions.UseMultipleWriteLocations {
				return &c.WriteLocations[i]
			}
		}
	}

	// We always fall back on the default endpoint, which should always be working for read and write
	return c.DefaultEndpoint
}

// Calling this function means that the endpoint should currently be considered 'down' and taken out of rotation.
// We set a timestamp on that unavailability so we can add it back when that time expires.
func (c *Client) markEndpointUnavailable(endpoint *CosmosEndpoint) {

	// We don't do this to the default endpoint, because then we might have no endpoints left.
	if endpoint.IsDefaultEndpoint {
		return
	}

	endpoint.IsUnavailable = true
	endpoint.UnavailableTimestamp = time.Now().Unix()
}

// purgeStaleEndpointUnavailability looks at every unavailable endpoint and determines if it should still be unavailable
// If the default unavailable time has passed
func (c *Client) purgeStaleEndpointUnavailability() {
	for i, endpoint := range c.ReadLocations {
		if endpoint.IsUnavailable {
			if time.Now().Unix()-(endpoint.UnavailableTimestamp) > int64(c.Config.RetryOptions.EndpointUnavailableTimeSec) {
				fmt.Printf("Marking endpoint %s available again\n", endpoint.EndpointName)
				c.ReadLocations[i].IsUnavailable = false
				c.ReadLocations[i].UnavailableTimestamp = 0
			}
		}
	}

	for i, endpoint := range c.WriteLocations {
		if endpoint.IsUnavailable {
			if time.Now().Unix()-(endpoint.UnavailableTimestamp) > int64(c.Config.RetryOptions.EndpointUnavailableTimeSec) {
				fmt.Printf("Marking endpoint %s available again\n", endpoint.EndpointName)
				c.WriteLocations[i].IsUnavailable = false
				c.WriteLocations[i].UnavailableTimestamp = 0
			}
		}
	}
}
