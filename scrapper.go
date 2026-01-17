package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

const TARGET_URL = "https://ca.1xbet.com/service-api/LiveBet/Open/GetCoupon"

const (
	MAX_WORKERS      = 60
	MAX_INFLIGHT     = 50
	REQUEST_TIMEOUT = 20 * time.Second
	BASE_BACKOFF    = 40 * time.Millisecond
	MAX_RETRIES     = 3
)

// ================= GLOBALS =================

var (
	TOTAL_REQUESTS  atomic.Int64
	FAILED_REQUESTS atomic.Int64
)

var PREFIXES = []string{"3J", "4J", "VJ", "L4", "N4"}

var SUFFIX = []string{
	"0", "1", "2", "3", "4", "5", "6", "7", "8", "9",
	"A", "B", "C", "D", "E", "F", "G", "H", "J", "K",
}

var USER_AGENTS = []string{
	"Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0",
	"Mozilla/5.0 (X11; Linux x86_64) Chrome/120.0.0.0",
	"Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6)",
	"Mozilla/5.0 (Windows NT 10.0; rv:121.0) Gecko/20100101 Firefox/121.0",
}

// ================= HTTP CLIENT =================

func newClient() *http.Client {
	tr := &http.Transport{
		MaxIdleConns:        300,
		MaxIdleConnsPerHost: 150,
		IdleConnTimeout:     90 * time.Second,
		ForceAttemptHTTP2:   true,
		DisableCompression:  false,
		DialContext: (&net.Dialer{
			Timeout:   50 * time.Second,
			KeepAlive: 300 * time.Second,
		}).DialContext,
	}

	return &http.Client{
		Transport: tr,
		Timeout:   REQUEST_TIMEOUT,
	}
}

// ================= DATABASE =================

func initDB() *sql.DB {
	db, err := sql.Open("sqlite3", "OUTPUT.db")
	if err != nil {
		panic(err)
	}

	db.Exec(`PRAGMA journal_mode=WAL;`)
	db.Exec(`PRAGMA synchronous=NORMAL;`)

	db.Exec(`
	CREATE TABLE IF NOT EXISTS codes (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		code TEXT UNIQUE,
		timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
	)`)

	return db
}

// ================= JOB =================

type Job struct {
	Code   string
	Client *http.Client
}

// ================= LIMITER =================

var limiter = make(chan struct{}, MAX_INFLIGHT)

// ================= REQUEST =================

func fetchCode(ctx context.Context, job Job) string {
	limiter <- struct{}{}
	defer func() { <-limiter }()

	payload := map[string]any{
		"Guid":    job.Code,
		"Lng":     "en",
		"partner": 1,
	}

	body, _ := json.Marshal(payload)

	req, _ := http.NewRequestWithContext(ctx, "POST", TARGET_URL, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/plain, */*")
	req.Header.Set("Accept-Language", "en-US,en;q=0.9")
	req.Header.Set("Origin", "https://ca.1xbet.com")
	req.Header.Set("Referer", "https://ca.1xbet.com/en")
	req.Header.Set("User-Agent", USER_AGENTS[rand.Intn(len(USER_AGENTS))])

	resp, err := job.Client.Do(req)
	TOTAL_REQUESTS.Add(1)

	if err != nil {
		FAILED_REQUESTS.Add(1)
		return "RETRY"
	}
	defer resp.Body.Close()

	if resp.StatusCode == 403 {
		return "RESET"
	}
	if resp.StatusCode != 200 {
		FAILED_REQUESTS.Add(1)
		return "RETRY"
	}

	var data map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return "RETRY"
	}

	if ok, _ := data["Success"].(bool); !ok {
		return "INVALID"
	}

	return "VALID"
}

// ================= WORKER =================

func worker(ctx context.Context, jobs <-chan Job, db *sql.DB) {
	for job := range jobs {
		backoff := BASE_BACKOFF

		for i := 0; i < MAX_RETRIES; i++ {
			res := fetchCode(ctx, job)

			switch res {
			case "VALID":
				fmt.Println("[VALID]", job.Code)
				db.Exec(`INSERT OR IGNORE INTO codes(code) VALUES(?)`, job.Code)
				goto NEXT

			case "INVALID":
				fmt.Println(job.Code, "INVALID")
				goto NEXT

			case "RESET":
				fmt.Println(job.Code, "RESET")
				time.Sleep(3 * time.Second)
				goto NEXT

			case "RETRY":
				jitter := time.Duration(rand.Intn(30)) * time.Millisecond
				time.Sleep(backoff + jitter)
			}
		}

	NEXT:
	}
}

// ================= GENERATOR =================

func generateJobs(jobs chan<- Job, clients map[string]*http.Client) {
	for _, p := range PREFIXES {
		for _, x := range SUFFIX {
			for _, a := range SUFFIX {
				for _, b := range SUFFIX {
					code := p + x + a + b
					jobs <- Job{Code: code, Client: clients[p]}
				}
			}
		}
	}
	close(jobs)
}

// ================= RPS =================

func rpsReporter(ctx context.Context) {
	t := time.NewTicker(30 * time.Second)
	defer t.Stop()

	var last int64
	for {
		select {
		case <-t.C:
			total := TOTAL_REQUESTS.Load()
			diff := total - last
			fmt.Printf("[RPS] %.2f | total=%d\n", float64(diff)/5, total)
			last = total
		case <-ctx.Done():
			return
		}
	}
}

// ================= MAIN =================

func main() {
	rand.Seed(time.Now().UnixNano())

	db := initDB()
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Hour)
	defer cancel()

	clients := make(map[string]*http.Client)
	for _, p := range PREFIXES {
		clients[p] = newClient()
	}

	jobs := make(chan Job, 5000)

	go rpsReporter(ctx)
	go generateJobs(jobs, clients)

	var wg sync.WaitGroup
	for i := 0; i < MAX_WORKERS; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			worker(ctx, jobs, db)
		}()
	}

	wg.Wait()
	fmt.Println("DONE")
}
