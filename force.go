package simpleforce

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
)

const (
	DefaultAPIVersion = "43.0"
	DefaultClientID   = "simpleforce"

	logPrefix = "[simpleforce]"
)

type Client interface {
	DescribeGlobal() (*SObjectMeta, error)
	DownloadFile(contentVersionID string, filepath string) error
	Query(q string) (*QueryResult, error)
	SObject(typeName ...string) *SObject
}

var _ Client = (*HTTPClient)(nil)

// HTTPClient is the main instance to access salesforce.
type HTTPClient struct {
	httpClient  *http.Client
	baseURL     string
	apiVersion  string
	instanceURL string
}

// NewHTTPClient creates a new instance of the client.
func NewHTTPClient(httpClient *http.Client, url, apiVersion string) *HTTPClient {
	client := &HTTPClient{
		httpClient: httpClient,
		apiVersion: apiVersion,
		baseURL:    url,
	}

	// Append "/" to the end of baseURL if not yet.
	if !strings.HasSuffix(client.baseURL, "/") {
		client.baseURL = client.baseURL + "/"
	}

	return client
}

// QueryResult holds the response data from an SOQL query.
type QueryResult struct {
	TotalSize      int       `json:"totalSize"`
	Done           bool      `json:"done"`
	NextRecordsURL string    `json:"nextRecordsUrl"`
	Records        []SObject `json:"records"`
}

// Query runs an SOQL query. q could either be the SOQL string or the nextRecordsURL.
func (h *HTTPClient) Query(q string) (*QueryResult, error) {
	var u string
	if strings.HasPrefix(q, "/services/data") {
		// q is nextRecordsURL.
		u = fmt.Sprintf("%s%s", h.instanceURL, q)
	} else {
		// q is SOQL.
		formatString := "%s/services/data/v%s/query?q=%s"
		baseURL := h.instanceURL

		u = fmt.Sprintf(formatString, baseURL, h.apiVersion, url.PathEscape(q))
	}

	data, err := h.httpRequest(http.MethodGet, u, nil)
	if err != nil {
		log.Println(logPrefix, "HTTP GET request failed:", u)
		return nil, err
	}

	var result QueryResult
	err = json.Unmarshal(data, &result)
	if err != nil {
		return nil, err
	}

	// Reference to client is needed if the object will be further used to do online queries.
	for idx := range result.Records {
		result.Records[idx].setClient(h)
	}

	return &result, nil
}

// SObject creates an SObject instance with provided type name and associate the SObject with the client.
func (h *HTTPClient) SObject(typeName ...string) *SObject {
	obj := &SObject{}
	obj.setClient(h)
	if typeName != nil {
		obj.setType(typeName[0])
	}
	return obj
}

// httpRequest executes an HTTP request to the salesforce server and returns the response data in byte buffer.
func (h *HTTPClient) httpRequest(method, url string, body io.Reader) ([]byte, error) {
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		return nil, err
	}

	req.Header.Add("Content-Type", "application/json")

	resp, err := h.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		log.Println(logPrefix, "request failed,", resp.StatusCode)
		buf := new(bytes.Buffer)
		buf.ReadFrom(resp.Body)
		newStr := buf.String()
		theError := ParseSalesforceError(resp.StatusCode, buf.Bytes())
		log.Println(logPrefix, "Failed resp.body: ", newStr)

		return nil, theError
	}

	return ioutil.ReadAll(resp.Body)
}

// makeURL generates a REST API URL based on baseURL, APIVersion of the client.
func (h *HTTPClient) makeURL(req string) string {
	h.apiVersion = strings.Replace(h.apiVersion, "v", "", -1)
	retURL := fmt.Sprintf("%s/services/data/v%s/%s", h.instanceURL, h.apiVersion, req)
	return retURL
}

// DownloadFile downloads a file based on the REST API path given. Saves to filePath.
func (h *HTTPClient) DownloadFile(contentVersionID string, filepath string) error {
	apiPath := fmt.Sprintf("/services/data/v%s/sobjects/ContentVersion/%s/VersionData", h.apiVersion, contentVersionID)
	baseURL := strings.TrimRight(h.baseURL, "/")
	url := fmt.Sprintf("%s%s", baseURL, apiPath)

	// Get the data
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	req.Header.Add("Content-Type", "application/json; charset=UTF-8")
	req.Header.Add("Accept", "application/json")

	// resp, err := http.Get(url)
	resp, err := h.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// Create the file
	out, err := os.Create(filepath)
	if err != nil {
		return err
	}
	defer out.Close()

	// Write the body to file
	_, err = io.Copy(out, resp.Body)
	return err
}

func parseHost(input string) string {
	parsed, err := url.Parse(input)
	if err == nil {
		return fmt.Sprintf("%s://%s", parsed.Scheme, parsed.Host)
	}
	return "Failed to parse URL input"
}

//Get the List of all available objects and their metadata for your organization's data
func (h *HTTPClient) DescribeGlobal() (*SObjectMeta, error) {
	apiPath := fmt.Sprintf("/services/data/v%s/sobjects", h.apiVersion)
	baseURL := strings.TrimRight(h.baseURL, "/")
	url := fmt.Sprintf("%s%s", baseURL, apiPath) // Get the objects

	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Add("Content-Type", "application/json; charset=UTF-8")
	req.Header.Add("Accept", "application/json")

	resp, err := h.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var meta SObjectMeta

	respData, err := ioutil.ReadAll(resp.Body)
	log.Println(logPrefix, fmt.Sprintf("status code %d", resp.StatusCode))
	if err != nil {
		log.Println(logPrefix, "error while reading all body")
	}

	err = json.Unmarshal(respData, &meta)
	if err != nil {
		return nil, err
	}
	return &meta, nil
}
