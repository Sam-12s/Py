package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"database/sql"
	"encoding/json"
	"fmt"
	"gopkg.in/gomail.v2"
	"math/rand"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"net/url"
	"time"
	"log"
	"io"
	"golang.org/x/net/http2"
	utls "github.com/refraction-networking/utls"
	
	_ "github.com/mattn/go-sqlite3"
)

var PROXIES = []string{
    "http://108.162.192.10:80",
    "http://104.16.0.111:80",
    "http://104.16.0.103:80",
    "http://104.16.0.102:80",
    "http://104.16.0.10:80",
    "http://104.16.0.100:80",
    "http://108.162.192.147:80",
    "http://108.162.192.0:80",
    "http://108.162.192.113:80",
    "http://108.162.192.116:80",
    "http://108.162.192.12:80",
    "http://104.16.0.0:80",
    "http://104.16.0.112:80",
    "http://108.162.192.134:80",
}

const TARGET_URL = "https://ca.1xbet.com/service-api/LiveBet/Open/GetCoupon"

var TLS_PROFILES = []utls.ClientHelloID{
	utls.HelloChrome_120,
	utls.HelloFirefox_120,
	utls.HelloChrome_Auto,
	utls.HelloFirefox_Auto,
}


var STOP_FLAG atomic.Bool

const (
	MAX_INFLIGHT    = 300
	REQUEST_TIMEOUT = 200 * time.Second
	BASE_BACKOFF    = 40 * time.Millisecond
	MAX_RETRIES     = 30
)

// ================= GLOBALS =================

var (
	TOTAL_REQUESTS      atomic.Int64 // all attempted
	SUCCESSFUL_REQUESTS atomic.Int64 // server accepted
	FAILED_REQUESTS     atomic.Int64 // server rejected or network fail
)

var proxyIndex atomic.Uint32

func startProxyRotator() {
    go func() {
        ticker := time.NewTicker(2 * time.Second)
        for range ticker.C {
            proxyIndex.Add(1)
        }
    }()
}

func currentProxy() *url.URL {
    idx := proxyIndex.Load() % uint32(len(PROXIES))
    p, _ := url.Parse(PROXIES[idx])
    return p
}
var PREFIXES = []string{"EG", "44", "AC", "PS", "AN", "J3", "8E", "6R", "79", "LJ", "U8", "V7", "CA", "4E", "AL", "2P", "HZ", "21", "JB", "5D", "K6", "SL", "PQ", "ZF", "K2"}

var SUFFIX = []string{"0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "A", "B", "C", "D", "E", "F", "G", "H", "J", "K", "L", "M", "N", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z"}

var USER_AGENTS = []string{
	"Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0",
	"Mozilla/5.0 (X11; Linux x86_64) Chrome/120.0.0.0",
	"Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6)",
	"Mozilla/5.0 (Windows NT 10.0; rv:121.0) Gecko/20100101 Firefox/121.0",
}

// ================= HTTP CLIENT =================

// newHTTP2Client returns an HTTP client using HTTP/2 + uTLS
func newGlobalClient() *http.Client {
    dialer := &net.Dialer{
        Timeout:   30 * time.Second,
        KeepAlive: 0, // 🔴 disable reuse
    }

    tr := &http.Transport{
        DisableKeepAlives: true,
        ForceAttemptHTTP2: false, // 🔴 important
        MaxIdleConns:      0,
        IdleConnTimeout:  0,

        Proxy: func(req *http.Request) (*url.URL, error) {
            return currentProxy(), nil
        },

        DialTLSContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
            rawConn, err := dialer.DialContext(ctx, network, addr)
            if err != nil {
                return nil, err
            }

            host, _, err := net.SplitHostPort(addr)
            if err != nil {
                rawConn.Close()
                return nil, err
            }

            cfg := &utls.Config{
                ServerName: host,
                NextProtos: []string{"http/1.1"},
            }

            uconn := utls.UClient(
                rawConn,
                cfg,
                TLS_PROFILES[rand.Intn(len(TLS_PROFILES))],
            )

            if err := uconn.Handshake(); err != nil {
                rawConn.Close()
                return nil, err
            }

            return uconn, nil
        },
    }

    return &http.Client{
        Transport: tr,
        Timeout:   REQUEST_TIMEOUT,
    }
}
func newClient() *http.Client {
	// Shared dialer

	tr := &http.Transport{
		DisableKeepAlives: false, // reuse connections for higher RPS
		MaxIdleConns:      100,
		MaxIdleConnsPerHost: 100,
		IdleConnTimeout:   90 * time.Second,

		ForceAttemptHTTP2: true, // allow HTTP/2 automatically

		DialTLSContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
		    	dialer := &net.Dialer{
                    Timeout:   50 * time.Second,
                    KeepAlive: 300 * time.Second,
                }
			rawConn, err := dialer.DialContext(ctx, network, addr)
			if err != nil {
				return nil, fmt.Errorf("tcp dial failed: %w", err)
			}

			host, _, err := net.SplitHostPort(addr)
			if err != nil {
				rawConn.Close()
				return nil, fmt.Errorf("split host port failed: %w", err)
			}

			// ⚡ Let uTLS pick ALPN automatically
			cfg := &utls.Config{
				ServerName: host,
				// NextProtos: nil  <- automatically picked by HelloID
			}

			uconn := utls.UClient(rawConn, cfg, TLS_PROFILES[rand.Intn(len(TLS_PROFILES))])

			// Retry handshake 3 times in case of network/TLS glitches
			var hErr error
			for i := 0; i < 3; i++ {
				if err := uconn.Handshake(); err != nil {
					hErr = err
					time.Sleep(50 * time.Millisecond)
					continue
				}
				hErr = nil
				break
			}

			if hErr != nil {
				rawConn.Close()
				return nil, fmt.Errorf("tls handshake failed: %w", hErr)
			}

			return uconn, nil
		},
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

	// Set PRAGMA options for speed and safety
	db.Exec(`PRAGMA journal_mode=WAL;`)
	db.Exec(`PRAGMA synchronous=NORMAL;`)
	db.Exec(`PRAGMA cache_size=-50000;`) // ~50MB
	db.Exec(`PRAGMA temp_store=MEMORY;`)
	db.Exec(`PRAGMA locking_mode=EXCLUSIVE;`)

	// Create table if not exists
	db.Exec(`
	CREATE TABLE IF NOT EXISTS codes (
		ID INTEGER PRIMARY KEY AUTOINCREMENT,
		Worker_Id TEXT,
		Label TEXT,
		Code TEXT UNIQUE,
		Teams TEXT,
		Events TEXT,
		Score TEXT,
		Time TEXT,
		Odds TEXT,
		Total_odds TEXT,
		Last_change TEXT,
		Timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
	)
	`)

	return db
}

// ================= JOB =================

type Job struct {
	Code   string
	Client *http.Client
}

// ================= REQUEST =================

func fetchCode(job Job, db *sql.DB, workerID string) (result string) {
	// ---------------- SAFETY NET ----------------
	defer func() {
		if r := recover(); r != nil {
			FAILED_REQUESTS.Add(1)
			result = "RETRY"
		}
	}()

	// ---------------- BUILD PAYLOAD ----------------
	payload := map[string]any{
		"Guid":    job.Code,
		"Lng":     "en",
		"partner": 1,
	}

	body, err := json.Marshal(payload)
	if err != nil {
		FAILED_REQUESTS.Add(1)
		return "RETRY"
	}

	req, err := http.NewRequest("POST", TARGET_URL, bytes.NewReader(body))
	if err != nil {
		FAILED_REQUESTS.Add(1)
		return "RETRY"
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/plain, */*")
	req.Header.Set("Accept-Language", "en-US,en;q=0.9")
	req.Header.Set("Origin", "https://ca.1xbet.com")
	req.Header.Set("Referer", "https://ca.1xbet.com/en")
	req.Header.Set("User-Agent", USER_AGENTS[rand.Intn(len(USER_AGENTS))])

	// ---------------- SEND REQUEST ----------------
	resp, err := GLOBAL_CLIENT.Do(req)
	
	

	if err != nil || resp == nil {
		FAILED_REQUESTS.Add(1)
		return "RETRY"
	}
	defer resp.Body.Close()

	// ---------------- STATUS HANDLING ----------------
	if resp.StatusCode == 403 {
		FAILED_REQUESTS.Add(1)
		return "RESET"
	}

	if resp.StatusCode != 200 {
		FAILED_REQUESTS.Add(1)
		return "RETRY"
	}

	// ---------------- DECODE JSON ----------------
	var data map[string]any
	dec := json.NewDecoder(resp.Body)
	dec.UseNumber()

	if err := dec.Decode(&data); err != nil || data == nil {
		FAILED_REQUESTS.Add(1)
		return "RETRY"
	}

	// ---------------- SUCCESS FLAG ----------------
	successVal, exists := data["Success"]
	success, ok := successVal.(bool)

	if !exists || !ok || !success {
		SUCCESSFUL_REQUESTS.Add(1)
		fmt.Println("INVALID", job.Code)
		return "INVALID"
	}

	SUCCESSFUL_REQUESTS.Add(1)
	fmt.Println("VALID", job.Code)

	// ---------------- VALUE OBJECT ----------------
	valueVal, exists := data["Value"]
	value, ok := valueVal.(map[string]any)
	if !exists || !ok || value == nil {
		return "VALID"
	}

	// ---------------- EVENTS ----------------
	eventsVal, exists := value["Events"]
	if !exists || eventsVal == nil {
		return "VALID"
	}

	eventsRaw, ok := eventsVal.([]any)
	if !ok || len(eventsRaw) == 0 {
		return "VALID"
	}

	// Python behavior: only process exactly 4 events
	if len(eventsRaw) != 4 {
		return "VALID"
	}

	// ---------------- FILTERS ----------------
	now := time.Now()
	allFootball := true
	allToday := true
	allUnfinished := true
	totalOdds := 1.0

	var (
		teamsArr  []string
		eventsArr []string
		scoreArr  []string
		timeArr   []string
		oddsArr   []string
	)

	for _, e := range eventsRaw {
		ev, ok := e.(map[string]any)
		if !ok || ev == nil {
			allFootball = false
			break
		}

		// Sport
		if sport, _ := ev["SportNameEng"].(string); sport != "Football" {
			allFootball = false
		}

		// Start time
		startNum, ok := ev["Start"].(json.Number)
		if !ok {
			allToday = false
			continue
		}

		startUnix, err := startNum.Int64()
		if err != nil {
			allToday = false
			continue
		}

		startTime := time.Unix(startUnix, 0)
		if startTime.Year() != now.Year() || startTime.YearDay() != now.YearDay() {
			allToday = false
		}

		// Finish flag
		if finished, ok := ev["Finish"].(bool); ok && finished {
			allUnfinished = false
		}

		// Odds
		if coef, ok := ev["Coef"].(json.Number); ok {
			f, err := coef.Float64()
			if err == nil {
				totalOdds *= f
				oddsArr = append(oddsArr, fmt.Sprintf("%.2f", f))
			}
		}

		// Strings (safe)
		teamsArr = append(teamsArr, fmt.Sprintf("%v vs %v", ev["Opp1"], ev["Opp2"]))
		eventsArr = append(eventsArr, fmt.Sprintf("%v", ev["GroupName"]))
		scoreArr = append(scoreArr, fmt.Sprintf("%v", ev["MarketName"]))
		timeArr = append(timeArr, startTime.Format("15:04"))
	}

	// ---------------- FINAL CONDITION ----------------
	if allFootball && allToday && allUnfinished && totalOdds > 50.0 {
		
		logCode(
			db,
			"4x",
			job.Code,
			workerID,
			strings.Join(teamsArr, "|"),
			strings.Join(eventsArr, "|"),
			strings.Join(scoreArr, "|"),
			strings.Join(timeArr, "|"),
			strings.Join(oddsArr, "|"),
			fmt.Sprintf("%.2f", totalOdds),
			"NA",
		)

		fmt.Println("FOUR ODD")
	}

	return "VALID"
}

func runtimeWatchdog(maxRuntime time.Duration) {
	start := time.Now()

	for {
		if time.Since(start) >= maxRuntime {
			fmt.Println("⏰ MAX EXECUTION TIME REACHED — STOPPING SCRIPT")
			STOP_FLAG.Store(true)
			return
		}
		time.Sleep(1 * time.Second)
	}
}

// ================= LOG CODE FUNCTION =================
func logCode(db *sql.DB, label, code, workerID, teams, events, score, times, odds, totalOdds, lastChange string) {
	_, err := db.Exec(`
		INSERT INTO codes (
			Worker_Id,
			Label,
			Code,
			Teams,
			Events,
			Score,
			Time,
			Odds,
			Total_odds,
			Last_change
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, workerID, label, code, teams, events, score, times, odds, totalOdds, lastChange)

	if err != nil {
		fmt.Println("DB insert error:", err)
		return
	}

	fmt.Printf("[Worker %s] => %s: %s (%s)\n", workerID, label, code, teams)
}

// ================= GENERATOR =================

func processPrefix(prefix string, db *sql.DB, workerID string) {
    for {
        if STOP_FLAG.Load() {
            return
        }

        // ✅ ONE client per prefix
        client := newHTTP2Client()
        fmt.Println("🔐 New client created for prefix:", prefix)

        var wg sync.WaitGroup
        resultChan := make(chan string, len(SUFFIX))

        for _, fourth := range SUFFIX {
            wg.Add(1)

            go func(f string) {
                defer wg.Done()

                res := fourthWorker(
                    prefix,
                    f,
                    client, // shared ONLY inside this prefix
                    db,
                    fmt.Sprintf("%s-%s", workerID, f),
                )

                resultChan <- res
            }(fourth)
        }

        // Wait for all workers
        wg.Wait()
        close(resultChan)

        // Check if TLS reset is needed
        needReset := false
        for r := range resultChan {
            if r == "RESET" {
                needReset = true
            }
        }

        // Close connections
        client.CloseIdleConnections()

        if needReset {
            fmt.Println("🔄 RESET detected — recreating client for prefix:", prefix)
            time.Sleep(3 * time.Second)
            continue // 🔁 recreate client
        }

        // Prefix fully completed
        return
    }
}

// ================= RPS =================

func rpsReporter(interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()

	var lastTotal, lastSuccess int64

	for {
		select {
		case <-t.C:
			total := TOTAL_REQUESTS.Load()
			success := SUCCESSFUL_REQUESTS.Load()

			diffTotal := total - lastTotal
			diffSuccess := success - lastSuccess

			fmt.Printf("[RPS] attempted: %.2f req/s | successful: %.2f req/s | total=%d | success=%d\n",
				float64(diffTotal)/interval.Seconds(),
				float64(diffSuccess)/interval.Seconds(),
				total, success,
			)

			lastTotal = total
			lastSuccess = success

			return
		}
	}
}

func sendDBViaGmail(dbPath string) {
	// Read credentials from environment variables
	senderEmail := os.Getenv("GMAIL_SENDER")
	appPassword := os.Getenv("GMAIL_APP_PASSWORD")
	recipientEmail := os.Getenv("GMAIL_RECEIVER")

	// Validate env vars
	if senderEmail == "" || appPassword == "" || recipientEmail == "" {
		fmt.Println("❌ Gmail credentials not set — skipping email")
		return
	}

	// Check DB file exists
	if _, err := os.Stat(dbPath); err != nil {
		fmt.Println("❌ DB file not found — skipping email:", err)
		return
	}

	// Create email
	m := gomail.NewMessage()
	m.SetHeader("From", senderEmail)
	m.SetHeader("To", recipientEmail)
	m.SetHeader("Subject", "SportyBet Script Output DB")
	m.SetBody(
		"text/plain",
		"Attached is the OUTPUT.db generated by the scraper.\n\nThis email was sent automatically.",
	)
	m.Attach(dbPath)

	// Gmail SMTP (SSL)
	d := gomail.NewDialer("smtp.gmail.com", 465, senderEmail, appPassword)

	// Send
	if err := d.DialAndSend(m); err != nil {
		fmt.Println("📧 Failed to send email:", err)
		return
	}

	fmt.Println("📧 OUTPUT.db sent successfully via Gmail.")
}

func fourthWorker(
	prefix string,
	fourth string,
	client *http.Client,
	db *sql.DB,
	workerID string,
) string {

	for _, a := range SUFFIX {
		for _, b := range SUFFIX {

			if STOP_FLAG.Load() {
				return "STOP"
			}

			code := prefix + fourth + a + b

			for {
				if STOP_FLAG.Load() {
					return "STOP"
				}

				res := fetchCode(
					Job{Code: code, Client: client},
					db,
					workerID,
				)

				switch res {
				case "VALID", "INVALID":
					goto NEXT_CODE

				case "RETRY":
					

				case "RESET":
					// tell prefix to reset TLS
					return "RESET"
				}
			}
		NEXT_CODE:
		}
	}

	return "DONE"
}
// ================= MAIN =================
var CLIENT *http.Client
func main() {
	log.SetOutput(io.Discard)
	startProxyRotator()
	// ---------------- DB ----------------
	db := initDB()
	defer db.Close()
	

	fmt.Println("STARTING PREFIX ENGINE")
	CLIENT = newGlobalClient()
	// ---------------- WATCHDOG ----------------
	// 5 hours 55 minutes = 355 minutes
	go runtimeWatchdog(355 * time.Minute)

	// ---------------- RPS REPORTER ----------------
	go rpsReporter(15 * time.Second)

	// ---------------- PREFIX WORKERS ----------------
	var wg sync.WaitGroup

	for i, prefix := range PREFIXES {
		wg.Add(1)

		go func(p string, id int) {
			defer wg.Done()

			workerID := fmt.Sprintf("PREFIX-%s-%d", p, id)
			fmt.Println("🔐 STARTING PREFIX", p)

			processPrefix(p, db, workerID)

			fmt.Println("🏁 PREFIX COMPLETED", p)
		}(prefix, i+1)
	}

	// ---------------- WAIT ----------------
	wg.Wait()

	fmt.Println("🛑 ALL PREFIXES STOPPED")

	// ---------------- EMAIL DB ----------------
	sendDBViaGmail("OUTPUT.db")

	fmt.Println("✅ PROGRAM EXITED CLEANLY")
}
