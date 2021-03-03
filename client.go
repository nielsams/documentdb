package documentdb

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
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

type Client struct {
	Url    string
	Config *Config
	http.Client

	ReadLocations  []EndpointLocation
	WriteLocations []EndpointLocation
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
	queryURL := c.getEndpointURL(EndpointType_ReadOnly) + "/" + link
	req, err = http.NewRequest(http.MethodPost, queryURL, buf)
	if err != nil {
		return nil, err
	}
	r := ResourceRequest(link, req)

	if err = c.apply(r, opts); err != nil {
		return nil, err
	}

	r.QueryHeaders(buf.Len())

	return c.do(r, expectStatusCode(http.StatusOK), ret)
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

// GetRegionalEndpoints sets the list of preferred read and write locations.
// It is called from documentDb.New() if the EnableEndpointDiscovery is set to true
func (c *Client) GetRegionalEndpoints() error {
	var (
		err          error
		req          *http.Request
		buf          = buffers.Get().(*bytes.Buffer)
		endpointDesc EndpointDescription
	)
	buf.Reset()
	defer buffers.Put(buf)

	req, err = http.NewRequest(http.MethodGet, c.Url+"/", buf)
	if err != nil {
		return err
	}
	r := ResourceRequest("", req)

	if err = c.apply(r, []CallOption{}); err != nil {
		return err
	}
	r.QueryHeaders(buf.Len())

	_, err = c.do(r, expectStatusCode(http.StatusOK), &endpointDesc)
	if err != nil {
		return err
	}

	// We set the received region list to the definitive region list as a whole, in case no changes are made for PreferredLocation
	c.ReadLocations = endpointDesc.ReadableLocations
	c.WriteLocations = endpointDesc.WritableLocations

	// We have a preferred region. This region should be moved to the front of the list if it is in the list
	prefLoc := c.Config.PreferredLocation

	// For both read and write, we look in the list for the PreferredLocation. If we find it, we move it to the front of the list
	for i, loc := range endpointDesc.ReadableLocations {
		if loc.EndpointName == prefLoc {
			c.ReadLocations = nil
			c.ReadLocations = append(c.ReadLocations, endpointDesc.ReadableLocations[i])
			c.ReadLocations = append(c.ReadLocations, endpointDesc.ReadableLocations[:i]...)
			c.ReadLocations = append(c.ReadLocations, endpointDesc.ReadableLocations[i+1:]...)
		}
	}
	for i, loc := range endpointDesc.WritableLocations {
		if loc.EndpointName == prefLoc {
			c.WriteLocations = nil
			c.WriteLocations = append(c.WriteLocations, endpointDesc.WritableLocations[i])
			c.WriteLocations = append(c.WriteLocations, endpointDesc.WritableLocations[:i]...)
			c.WriteLocations = append(c.WriteLocations, endpointDesc.WritableLocations[i+1:]...)
		}
	}

	return nil
}

// Private generic method resource
func (c *Client) method(method string, link string, validator statusCodeValidatorFunc, ret interface{}, body *bytes.Buffer, opts ...CallOption) (*Response, error) {
	var queryURL string

	// With a GET request we only need the read endpoint. For others we get a readwrite endpoint
	// Note that for 'Query', the queryurl is set elsewhere.
	switch method {
	case http.MethodGet:
		queryURL = c.getEndpointURL(EndpointType_ReadOnly) + "/" + link
	default:
		queryURL = c.getEndpointURL(EndpointType_ReadWrite) + "/" + link
	}

	req, err := http.NewRequest(method, queryURL, body)
	if err != nil {
		return nil, err
	}

	r := ResourceRequest(link, req)

	if err = c.apply(r, opts); err != nil {
		return nil, err
	}

	return c.do(r, validator, ret)
}

// Private Do function, DRY
func (c *Client) do(r *Request, validator statusCodeValidatorFunc, data interface{}) (*Response, error) {
	fmt.Println("Outgoing request: ", r.Request.URL)
	resp, err := c.Do(r.Request)
	if err != nil {
		return nil, err
	}
	if !validator(resp.StatusCode) {
		err = &RequestError{}
		readJson(resp.Body, &err)
		return nil, err
	}
	defer resp.Body.Close()
	if data == nil {
		return nil, nil
	}
	return &Response{resp.Header}, readJson(resp.Body, data)
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

// getEndpointURL returns the endpoint that will be used in the ongoing call.
// By specifying either EndpointType_ReadOnly or EndpointType_ReadWrite, we select the top endpoint from that list.
// If no multiregion config/setup is used, the ReadLocations and WriteLocations lists are empty and the default c.Url will be returned
func (c *Client) getEndpointURL(endpointType EndpointType) string {
	switch endpointType {

	case EndpointType_ReadOnly:
		if len(c.ReadLocations) > 0 {
			return c.ReadLocations[0].EndpointURL
		}
	case EndpointType_ReadWrite:
		if len(c.WriteLocations) > 0 && c.Config.UseMultipleWriteLocations {
			return c.WriteLocations[0].EndpointURL
		}
	}
	return c.Url
}
