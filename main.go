package main

import (
	"bytes"
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
	"time"

	_ "github.com/mattn/go-sqlite3"
)

const TARGET_URL = "https://ca.1xbet.com/service-api/LiveBet/Open/GetCoupon"

var STOP_FLAG atomic.Bool

const (
	MAX_INFLIGHT    = 80
	REQUEST_TIMEOUT = 20 * time.Second
	BASE_BACKOFF    = 40 * time.Millisecond
	MAX_RETRIES     = 3
)

// ================= GLOBALS =================

var (
	TOTAL_REQUESTS      atomic.Int64 // all attempted
	SUCCESSFUL_REQUESTS atomic.Int64 // server accepted
	FAILED_REQUESTS     atomic.Int64 // server rejected or network fail
)

var PREFIXES = []string{"EG", "44", "AC", "PS", "AN", "J3", "8E", "6R", "79", "LJ", "U8", "V7", "CA", "4E", "AL", "2P", "HZ", "21", "JB", "5D", "K6", "SL", "PQ", "ZF", "K2"}

var SUFFIX = []string{"0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "A", "B", "C", "D", "E", "F", "G", "H", "J", "K", "L", "M", "N", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z"}

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

func fetchCode(job Job, db *sql.DB, workerID string) string {
	// -------- request --------
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

	resp, err := job.Client.Do(req)
	TOTAL_REQUESTS.Add(1)

	if err != nil {
		FAILED_REQUESTS.Add(1)
		return "RETRY"
	}
	defer resp.Body.Close()

	// -------- status handling --------
	if resp.StatusCode == 403 {
		FAILED_REQUESTS.Add(1)
		return "RESET"
	}
	if resp.StatusCode != 200 {
		FAILED_REQUESTS.Add(1)
		return "RETRY"
	}

	// -------- decode --------
	var data map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		FAILED_REQUESTS.Add(1)
		return "RETRY"
	}

	success, ok := data["Success"].(bool)
	if !ok || !success {
		SUCCESSFUL_REQUESTS.Add(1)
		fmt.Println("INVALID",job.Code)
		return "INVALID"
	}

	// server accepted
	SUCCESSFUL_REQUESTS.Add(1)
	fmt.Println("VALID",job.Code)
	// -------- value / events --------
	value, ok := data["Value"].(map[string]any)
	if !ok {
		return "VALID"
	}

	eventsRaw, ok := value["Events"].([]any)
	if !ok || len(eventsRaw) == 0 {
		return "VALID"
	}

	// python behavior: if not exactly 4 → VALID (no log)
	if len(eventsRaw) != 4 {
		return "VALID"
	}

	// -------- filters --------
	allFootball := true
	allToday := true
	allUnfinished := true
	totalOdds := 1.0
	now := time.Now()

	var (
		teamsArr  []string
		eventsArr []string
		scoreArr  []string
		timeArr   []string
		oddsArr   []string
	)

	for _, e := range eventsRaw {
		ev, ok := e.(map[string]any)
		if !ok {
			allFootball = false
			break
		}

		// sport
		if ev["SportNameEng"] != "Football" {
			allFootball = false
		}

		// date
		start, _ := ev["Start"].(float64)
		startTime := time.Unix(int64(start), 0)
		if startTime.YearDay() != now.YearDay() || startTime.Year() != now.Year() {
			allToday = false
		}

		// finish
		if finished, ok := ev["Finish"].(bool); ok && finished {
			allUnfinished = false
		}

		// odds
		if coef, ok := ev["Coef"].(float64); ok {
			totalOdds *= coef
			oddsArr = append(oddsArr, fmt.Sprintf("%.2f", coef))
		}

		// strings
		teamsArr = append(teamsArr,
			fmt.Sprintf("%v vs %v", ev["Opp1"], ev["Opp2"]),
		)
		eventsArr = append(eventsArr, fmt.Sprintf("%v", ev["GroupName"]))
		scoreArr = append(scoreArr, fmt.Sprintf("%v", ev["MarketName"]))
		timeArr = append(timeArr, startTime.Format("15:04"))
	}

	// -------- final condition (same as Python) --------
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

		client := newClient()
		

		var wg sync.WaitGroup
		resultChan := make(chan string, len(SUFFIX))

		for _, fourth := range SUFFIX {
			wg.Add(1)

			go func(f string) {
				defer wg.Done()

				res := fourthWorker(
					prefix,
					f,
					&client,
					db,
					fmt.Sprintf("%s-%s", workerID, f),
				)

				resultChan <- res
			}(fourth)
		}

		// wait for all fourth workers
		wg.Wait()
		close(resultChan)

		// check results
		needReset := false
		for r := range resultChan {
			if r == "RESET" {
				needReset = true
			}
		}

		client.CloseIdleConnections()

		if needReset {
			fmt.Println("🔄 403 detected — resetting TLS for prefix", prefix)
			time.Sleep(5 * time.Second)
			continue // restart prefix with new client
		}

		// normal completion
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
	client **http.Client,
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
					Job{Code: code, Client: *client},
					db,
					workerID,
				)

				switch res {
				case "VALID", "INVALID":
					goto NEXT_CODE

				case "RETRY":
					time.Sleep(time.Duration(rand.Intn(2)+1) * time.Second)

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

func main() {
	// ---------------- DB ----------------
	db := initDB()
	defer db.Close()

	fmt.Println("STARTING PREFIX ENGINE")

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
