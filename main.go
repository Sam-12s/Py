package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"database/sql"
	_ "github.com/mattn/go-sqlite3"
    "bytes"
    "encoding/base64"
    "log"
    "os"
    "net/smtp"
    "strconv"

	utls "github.com/refraction-networking/utls"
)

/* =========================
   CONFIG (FROM PY SCRIPT)
========================= */
const MAX_RUNTIME_MINUTES = 355
var stopChan = make(chan struct{})
var prefixSem chan struct{}




var PREFIXES = []string{"L4X"}

var SUFFIX_CHARS = []rune("0123456789ABCDEFGHJKLMNPQRSTUVWXYZ")
var MAX_PREFIX_CONCURRENCY = len(PREFIXES)
const MAX_INITIAL_INVALID = 40
var FOURTH_CHARS = []rune("0123456789ABCDEFGHJKLMNPQRSTUVWXYZ")

/* =========================
   USER AGENTS (ROTATED)
========================= */

var USER_AGENTS = []string{
	"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
	"Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
	"Mozilla/5.0 (X11; Linux x86_64) Gecko/20100101 Firefox/118.0",
	"Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 Mobile/15E148 Safari/604.1",
}
func runtimeWatchdog() {
	endTime := time.Now().Add(time.Minute * MAX_RUNTIME_MINUTES)

	for {
		if time.Now().After(endTime) {
			fmt.Println("⏰ MAX EXECUTION TIME REACHED — STOPPING")
			close(stopChan) // 🔴 global stop
			return
		}
		time.Sleep(1 * time.Second)
	}
}

/* =========================
   SPORTYBET RESPONSE
========================= */

type SportyBetResponse struct {
	Message string `json:"message"`

	Data struct {
		Outcomes []Outcome `json:"outcomes"`
	} `json:"data"`
}

type Outcome struct {
	MatchStatus string `json:"matchStatus"`

	HomeTeamName string `json:"homeTeamName"`
	AwayTeamName string `json:"awayTeamName"`

	EstimateStartTime int64 `json:"estimateStartTime"`

	Sport struct {
		Category struct {
			Name string `json:"name"`
		} `json:"category"`
	} `json:"sport"`

	Markets []Market `json:"markets"`
}

type Market struct {
	Desc string `json:"desc"`

	LastOddsChangeTime int64 `json:"lastOddsChangeTime"`

	Outcomes []MarketOutcome `json:"outcomes"`
}

type MarketOutcome struct {
	Desc string `json:"desc"`
	Odds string `json:"odds"`
}
func MatchDate(o *Outcome) time.Time {
	if o == nil {
		return time.Time{}
	}
	return time.UnixMilli(o.EstimateStartTime).Local()
}

func IsToday(o *Outcome) bool {
	matchDate := MatchDate(o)
	now := time.Now()
	y1, m1, d1 := matchDate.Date()
	y2, m2, d2 := now.Date()
	return y1 == y2 && m1 == m2 && d1 == d2
}

func (o *Outcome) MatchStatusSafe() string {
	if o == nil {
		return ""
	}
	return o.MatchStatus
}

func (o *Outcome) CategorySafe() string {
	if o == nil {
		return ""
	}
	return o.Sport.Category.Name
}

func (o *Outcome) EventDescSafe() string {
	m := o.Market0()
	if m == nil {
		return ""
	}
	return m.Desc
}

func (o *Outcome) OddsSafe() (float64, bool) {
	mo := o.MarketOutcome0()
	if mo == nil {
		return 0, false
	}
	val, err := strconv.ParseFloat(mo.Odds, 64)
	if err != nil {
		return 0, false
	}
	return val, true
}

func (o *Outcome) ScoreDescSafe() string {
	mo := o.MarketOutcome0()
	if mo == nil {
		return ""
	}
	return mo.Desc
}

func (o *Outcome) OddsChangeTimeSafe() time.Time {
	m := o.Market0()
	if m == nil {
		return time.Time{}
	}
	return time.UnixMilli(m.LastOddsChangeTime).Local()
}

func CalcOdds2(o1, o2 *Outcome) (float64, bool) {
	odd1, ok1 := o1.OddsSafe()
	odd2, ok2 := o2.OddsSafe()
	if !ok1 || !ok2 {
		return 0, false
	}
	return odd1 * odd2, true
}

func CalcOdds3(o1, o2, o3 *Outcome) (float64, bool) {
	odd1, ok1 := o1.OddsSafe()
	odd2, ok2 := o2.OddsSafe()
	odd3, ok3 := o3.OddsSafe()
	if !ok1 || !ok2 || !ok3 {
		return 0, false
	}
	return odd1 * odd2 * odd3, true
}


func IsPlayable(o *Outcome) bool {
	status := o.MatchStatusSafe()
	return status == "Not start" || status == "H1"
}

func IsNotSimulated(o *Outcome) bool {
	return o.CategorySafe() != "Simulated Reality League"
}

func (o *Outcome) Market0() *Market {
	if o == nil || len(o.Markets) == 0 {
		return nil
	}
	return &o.Markets[0]
}

func (o *Outcome) MarketOutcome0() *MarketOutcome {
	m := o.Market0()
	if m == nil || len(m.Outcomes) == 0 {
		return nil
	}
	return &m.Outcomes[0]
}

/* =========================
   uTLS CLIENT (ANTI-BOT)
========================= */

func newSportyClient() *http.Client {
	return &http.Client{
		Timeout: 25 * time.Second,
		Transport: &http.Transport{
		    DialContext: (&net.Dialer{
                Timeout:   50 * time.Second,
                KeepAlive: 60 * time.Second,
            }).DialContext,

			DialTLSContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
				raw, err := net.DialTimeout("tcp", addr, 10*time.Second)
				if err != nil {
					return nil, err
				}

				cfg := &utls.Config{
				    InsecureSkipVerify: true,
					ServerName: strings.Split(addr, ":")[0],
					NextProtos: []string{"http/1.1"},
				}

				uconn := utls.UClient(raw, cfg, utls.HelloChrome_120)
				if err := uconn.Handshake(); err != nil {
					return nil, err
				}
				return uconn, nil
			},
		},
	}
}

func initDB(dbName string) (*sql.DB, error) {
	db, err := sql.Open("sqlite3", dbName)
	if err != nil {
		return nil, err
	}

	pragmas := []string{
		"PRAGMA journal_mode=WAL;",
		"PRAGMA synchronous=NORMAL;",
		"PRAGMA cache_size=-50000;",
		"PRAGMA temp_store=MEMORY;",
		"PRAGMA locking_mode=EXCLUSIVE;",
	}

	for _, p := range pragmas {
		if _, err := db.Exec(p); err != nil {
			return nil, err
		}
	}

	createTable := `
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
	);`

	if _, err := db.Exec(createTable); err != nil {
		return nil, err
	}

	return db, nil
}
/* =========================
   FETCH + PARSE (AV16 LOGIC)
========================= */

const (
	RES_403    = "ERROR_403"
	RES_RETRY = "ERROR_RETRY"
	RES_INVALID = "INVALID"
	RES_VALID = "VALID"
	RES_SINGLE = "SINGLE"
	RES_DOUBLE = "DOUBLE"
	RES_TRIPLE = "TRIPLE"
)

type LogPayload struct {
	Teams       string
	Events      string
	Score       string
	Time        string
	Odds        string
	TotalOdds  string
	LastChange string
}
func fetchCode(client *http.Client, code string) (string, *LogPayload) {
	url := fmt.Sprintf(
		"https://www.sportybet.com/api/ng/orders/share/%s?_t=%d",
		code,
		time.Now().UnixMilli(),
	)
	todayDate := time.Now().Local().Format("2006-01-02")

	req, _ := http.NewRequest("GET", url, nil)
	req.Header.Set("User-Agent", USER_AGENTS[rand.Intn(len(USER_AGENTS))])
	req.Header.Set("Accept", "*/*")
	req.Header.Set("Accept-Language", "en")
	req.Header.Set("Cache-Control", "no-cache")
	req.Header.Set("Pragma", "no-cache")
	req.Header.Set("Referer", "https://www.sportybet.com/ng/")
	req.Header.Set("Origin", "https://www.sportybet.com")
	req.Header.Set("Connection", "keep-alive")

	resp, err := client.Do(req)
	if err != nil {
		return RES_RETRY, nil
	}
	defer resp.Body.Close()

	// ─── EXACT PYTHON BEHAVIOR ───
	if resp.StatusCode == 403 {
	    fmt.Println("rsp:",resp.StatusCode)
		return RES_403, nil
	}
	if resp.StatusCode != 200 {
	    fmt.Println("rsp:",resp.StatusCode)
		return RES_RETRY, nil
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return RES_RETRY, nil
	}

	var parsed SportyBetResponse
	if err := json.Unmarshal(body, &parsed); err != nil {
		return RES_RETRY, nil
	}

	// ─── MESSAGE HANDLING ───
	if parsed.Message == "The code is invalid." {
	    fmt.Println("The code is invalid.",code)
		return RES_INVALID, nil
	}
	if parsed.Message != "Success" {
	    fmt.Println("No usable outcomes.",code)
		return RES_VALID, nil
	}

	outcomes := parsed.Data.Outcomes
	n := len(outcomes)

	if n == 0 || n > 3 {
	    fmt.Println("RETRY------.",code)
		return RES_VALID, nil
	}

	// ─────────────────────────────
	// TRIPLE
	// ─────────────────────────────
	if n == 3 {
		o1 := &outcomes[0]
        o2 := &outcomes[1]
        o3 := &outcomes[2]


		valid := o1.MatchStatusSafe()
		matchDate0 := MatchDate(o1).Format("2006-01-02")
		matchDate1 := MatchDate(o2).Format("2006-01-02")
		matchDate2 := MatchDate(o3).Format("2006-01-02")

		types0 := o1.CategorySafe()
		types1 := o2.CategorySafe()
		types2 := o3.CategorySafe()

		odd1, ok1 := o1.OddsSafe()
		odd2, ok2 := o2.OddsSafe()
		odd3, ok3 := o3.OddsSafe()
		if !ok1 || !ok2 || !ok3 {
			return RES_RETRY, nil
		}

		totalOdds, ok := CalcOdds3(o1, o2, o3)
		if !ok {
			return RES_RETRY, nil
		}

		if (valid == "Not start" || valid == "H1") &&
			matchDate0 == todayDate &&
			matchDate1 == todayDate &&
			matchDate2 == todayDate &&
			totalOdds > 15.00 &&
			types0 != "Simulated Reality League" &&
			types1 != "Simulated Reality League" &&
			types2 != "Simulated Reality League" {
            fmt.Println("TRIPLE.",code)
			payload := &LogPayload{
				Teams: fmt.Sprintf(
					"%s vs %s|%s vs %s|%s vs %s",
					o1.HomeTeamName, o1.AwayTeamName,
					o2.HomeTeamName, o2.AwayTeamName,
					o3.HomeTeamName, o3.AwayTeamName,
				),
				Events: fmt.Sprintf(
					"%s|%s|%s",
					o1.EventDescSafe(),
					o2.EventDescSafe(),
					o3.EventDescSafe(),
				),
				Score: fmt.Sprintf(
					"%s|%s|%s",
					o1.ScoreDescSafe(),
					o2.ScoreDescSafe(),
					o3.ScoreDescSafe(),
				),
				Time: fmt.Sprintf(
					"%s||%s||%s",
					MatchDate(o1).Format("15:04:05"),
					MatchDate(o2).Format("15:04:05"),
					MatchDate(o3).Format("15:04:05"),
				),
				Odds: fmt.Sprintf(
					"%.2f|%.2f|%.2f",
					odd1, odd2, odd3,
				),
				TotalOdds: fmt.Sprintf("%.2f", totalOdds),
				LastChange: fmt.Sprintf(
					"%s||%s||%s",
					o1.OddsChangeTimeSafe().Format("15:04:05"),
					o2.OddsChangeTimeSafe().Format("15:04:05"),
					o3.OddsChangeTimeSafe().Format("15:04:05"),
				),
			}

			return RES_TRIPLE, payload
		}

		return RES_VALID, nil
	}

	// ─────────────────────────────
	// DOUBLE
	// ─────────────────────────────
	if n == 2 {
		o1 := &outcomes[0]
        o2 := &outcomes[1]



		valid := o1.MatchStatusSafe()
		event0 := o1.EventDescSafe()
		event1 := o2.EventDescSafe()

		matchDate0 := MatchDate(o1).Format("2006-01-02")
		matchDate1 := MatchDate(o2).Format("2006-01-02")

		types0 := o1.CategorySafe()
		types1 := o2.CategorySafe()

		odd1, ok1 := o1.OddsSafe()
		odd2, ok2 := o2.OddsSafe()
		if !ok1 || !ok2 {
			return RES_RETRY, nil
		}

		if (valid == "Not start" || valid == "H1") &&
			event0 == "Correct Score" &&
			event1 == "Correct Score" &&
			matchDate0 == todayDate &&
			matchDate1 == todayDate &&
			types0 != "Simulated Reality League" &&
			types1 != "Simulated Reality League" {
            fmt.Println("DOUBLE.",code)
			totalOdds, ok := CalcOdds2(o1, o2)
			if !ok {
				return RES_RETRY, nil
			}

			payload := &LogPayload{
				Teams: fmt.Sprintf(
					"%s vs %s|%s vs %s",
					o1.HomeTeamName, o1.AwayTeamName,
					o2.HomeTeamName, o2.AwayTeamName,
				),
				Events: fmt.Sprintf("%s|%s", event0, event1),
				Score:  fmt.Sprintf("%s|%s", o1.ScoreDescSafe(), o2.ScoreDescSafe()),
				Time: fmt.Sprintf(
					"%s||%s",
					MatchDate(o1).Format("15:04:05"),
					MatchDate(o2).Format("15:04:05"),
				),
				Odds: fmt.Sprintf("%.2f|%.2f", odd1, odd2),
				TotalOdds: fmt.Sprintf("%.2f", totalOdds),
				LastChange: fmt.Sprintf(
					"%s||%s",
					o1.OddsChangeTimeSafe().Format("15:04:05"),
					o2.OddsChangeTimeSafe().Format("15:04:05"),
				),
			}

			return RES_DOUBLE, payload
		}

		return RES_VALID, nil
	}

	// ─────────────────────────────
	// SINGLE
	// ─────────────────────────────
	if n == 1 {
		o := outcomes[0]

		valid := o.MatchStatusSafe()
		event := o.EventDescSafe()
		matchDate := MatchDate(o).Format("2006-01-02")
		types := o.CategorySafe()

		odd, ok := o.OddsSafe()
		if !ok {
			return RES_RETRY, nil
		}

		if (valid == "Not start" || valid == "H1") &&
			event == "Correct Score" &&
			matchDate == todayDate &&
			types != "Simulated Reality League" {
            fmt.Println("SINGLE.",code)
			payload := &LogPayload{
				Teams:      fmt.Sprintf("%s vs %s", o.HomeTeamName, o.AwayTeamName),
				Events:     event,
				Score:      o.ScoreDescSafe(),
				Time:       MatchDate(o).Format("15:04:05"),
				Odds:       fmt.Sprintf("%.2f", odd),
				TotalOdds:  fmt.Sprintf("%.2f", odd),
				LastChange: o.OddsChangeTimeSafe().Format("15:04:05"),
			}

			return RES_SINGLE, payload
		}

		return RES_VALID, nil
	}

	return RES_VALID, nil
}

/* =========================
   FOURTH WORKER (PY CLONE)
========================= */
var (
	SMTP_HOST = "smtp.gmail.com"
	SMTP_PORT = "587"

	SMTP_USER = os.Getenv("GMAIL_SENDER")
	SMTP_PASS = os.Getenv("GMAIL_APP_PASSWORD")

	EMAIL_FROM = os.Getenv("GMAIL_SENDER")
	EMAIL_TO   = os.Getenv("GMAIL_RECEIVER")
)

func sendOutputEmail(dbPath string) error {
	fileData, err := os.ReadFile(dbPath)
	if err != nil {
		return err
	}

	boundary := "SPORTYBET-BOUNDARY"

	headers := map[string]string{
		"From":         EMAIL_FROM,
		"To":           EMAIL_TO,
		"Subject":      "SportyBet Output",
		"MIME-Version": "1.0",
		"Content-Type": "multipart/mixed; boundary=" + boundary,
	}

	var msg bytes.Buffer

	for k, v := range headers {
		msg.WriteString(fmt.Sprintf("%s: %s\r\n", k, v))
	}
	msg.WriteString("\r\n")

	// 📄 Email body
	msg.WriteString("--" + boundary + "\r\n")
	msg.WriteString("Content-Type: text/plain; charset=utf-8\r\n\r\n")
	msg.WriteString("Attached is the output database.\r\n\r\n")

	// 📎 Attachment
	msg.WriteString("--" + boundary + "\r\n")
	msg.WriteString("Content-Type: application/octet-stream\r\n")
	msg.WriteString("Content-Disposition: attachment; filename=\"output.db\"\r\n")
	msg.WriteString("Content-Transfer-Encoding: base64\r\n\r\n")

	encoded := make([]byte, base64.StdEncoding.EncodedLen(len(fileData)))
	base64.StdEncoding.Encode(encoded, fileData)

	// wrap base64 at 76 chars (RFC compliant)
	for i := 0; i < len(encoded); i += 76 {
		end := i + 76
		if end > len(encoded) {
			end = len(encoded)
		}
		msg.Write(encoded[i:end])
		msg.WriteString("\r\n")
	}

	msg.WriteString("--" + boundary + "--")

	auth := smtp.PlainAuth(
		"",
		SMTP_USER,
		SMTP_PASS,
		SMTP_HOST,
	)

	return smtp.SendMail(
		SMTP_HOST+":"+SMTP_PORT,
		auth,
		EMAIL_FROM,
		[]string{EMAIL_TO},
		msg.Bytes(),
	)
}


func fourthWorker(
	prefix string,
	fourth rune,
	startIndex int,
	step int,
	client *http.Client,
	workerID string,
	db *sql.DB,
) string {



	invalidCount := 0
	countingEnabled := true

	// 🔹 split 5th char space exactly like Python
	for i := startIndex; i < len(SUFFIX_CHARS); i += step {
        select {
            case <-stopChan:
                return "OK"
            default:
            }

		a := SUFFIX_CHARS[i]

		for _, b := range SUFFIX_CHARS {
			code := fmt.Sprintf("%s%c%c%c", prefix, fourth, a, b)

			var result string

			// 🔁 EXACT retry loop
			for {
				var payload *LogPayload
                result, payload = fetchCode(client, code)


				if result == RES_403 {
					fmt.Printf("[%s] 🔄 403 on %s\n", workerID, code)
					return "NEED_CLIENT_RESET"
				}

				if result == RES_RETRY {
					continue
				}

				break
			}

			// 🔐 INVALID / UNLOCK logic
			if countingEnabled {
				if result == RES_INVALID {
					invalidCount++
					if invalidCount >= MAX_INITIAL_INVALID {
						fmt.Printf("[%s] 🛑 stopped after INVALID limit", workerID)
						return "OK"
					}
				} else if result == RES_VALID {
					countingEnabled = false
					invalidCount = 0
					fmt.Printf("[%s] 🔓 unlocked", workerID)
				}
			}


            if result == RES_SINGLE || result == RES_DOUBLE || result == RES_TRIPLE {
                if payload != nil {
                    _ = logCode(
                        db,
                        workerID,
                        result,
                        code,
                        payload.Teams,
                        payload.Events,
                        payload.Score,
                        payload.Time,
                        payload.Odds,
                        payload.TotalOdds,
                        payload.LastChange,
                    )
                }
            }


			// 🕒 jitter (human-like)
			time.Sleep(time.Duration(rand.Intn(120)+80) * time.Millisecond)
		}
	}


	return "OK"
}

/* =========================
   PREFIX ENGINE
========================= */

func processPrefix(prefix string, db *sql.DB) {
	// Acquire prefix semaphore (Python asyncio.Semaphore)
	select {
	case prefixSem <- struct{}{}:
		defer func() { <-prefixSem }()
	case <-stopChan:
		return
	}

	fmt.Println("▶️ Starting prefix:", prefix)

	// Loop until prefix finishes or stop signal
	for {
		select {
		case <-stopChan:
			fmt.Println("⏹ Prefix stopped:", prefix)
			return
		default:
		}

		// Create fresh TLS client (Python resets on demand)
		client := newSportyClient()
		if client == nil {
			fmt.Println("❌ TLS client creation failed:")
			time.Sleep(2 * time.Second)
			continue
		}

		// Fourth-character workers: A–Z0–9 (example set)
		fourthChars := FOURTH_CHARS

		var needReset int32 // 0 = false, 1 = true

		var wg sync.WaitGroup

		for _, f := range fourthChars {
			// First worker: startIndex = 0
			wg.Add(1)
			go func(ch rune) {
				defer wg.Done()

				workerID := fmt.Sprintf("%s-%c-0", prefix, ch)

				if res := fourthWorker(
					prefix,
					ch,
					0, // startIndex
					2, // step
					client,
					workerID,
					db,
				); res == "NEED_CLIENT_RESET" {
					atomic.StoreInt32(&needReset, 1)

				}
			}(f)

			// Second worker: startIndex = 1 (THIS IS THE “SAME FOR SECOND WORKER”)
			wg.Add(1)
			go func(ch rune) {
				defer wg.Done()

				workerID := fmt.Sprintf("%s-%c-1", prefix, ch)

				if res := fourthWorker(
					prefix,
					ch,
					1, // startIndex (different!)
					2, // step
					client,
					workerID,
					db,
				); res == "NEED_CLIENT_RESET" {
					atomic.StoreInt32(&needReset, 1)

				}
			}(f)
		}

		// Wait for all fourth workers to finish
		wg.Wait()

		// If any worker triggered TLS reset (403), restart prefix loop
		if atomic.LoadInt32(&needReset) == 1 {
			fmt.Println("🔄 TLS reset for prefix:", prefix)
			time.Sleep(1 * time.Second)
			continue
		}

		// Prefix completed normally (Python: break out)
		fmt.Println("✅ Prefix completed:", prefix)
		return
	}
}

func logCode(
	db *sql.DB,
	workerID string,
	label string,
	code string,
	teams string,
	events string,
	score string,
	times string,
	odds string,
	totalOdds string,
	lastChange string,
) error {

	_, err := db.Exec(`
	INSERT OR IGNORE INTO codes (
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
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
	`,
		workerID,
		label,
		code,
		teams,
		events,
		score,
		times,
		odds,
		totalOdds,
		lastChange,
	)

	return err
}

/* =========================
   MAIN
========================= */
func main() {
	// --- Initialize database ---
	db, err := initDB("output.db")
	if err != nil {
		log.Fatal("DB init failed:", err)
	}
	defer db.Close()

	// SQLite behavior must match Python
	db.SetMaxOpenConns(1)

	// --- Initialize prefix semaphore globally ---
	// processPrefix() will acquire/release it
	prefixSem = make(chan struct{}, MAX_PREFIX_CONCURRENCY)

	// --- Start runtime watchdog ---
	go runtimeWatchdog()

	// --- Launch prefix processors ---
	var prefixWG sync.WaitGroup

	for _, prefix := range PREFIXES {
		prefixWG.Add(1)

		go func(p string) {
			defer prefixWG.Done()
			processPrefix(p, db)
		}(prefix)
	}

	// --- Close stopChan when all prefixes finish ---
	go func() {
		prefixWG.Wait()
		select {
		case <-stopChan:
			// already closed by watchdog
		default:
			close(stopChan)
		}
	}()

	// --- Block until watchdog OR prefixes complete ---
	<-stopChan

	fmt.Println("🏁 All processing finished")

	// --- Send output.db via email (once, Python-style) ---
	fmt.Println("📤 Sending output.db via email...")
	if err := sendOutputEmail("output.db"); err != nil {
		fmt.Println("❌ Email send failed:", err)
	} else {
		fmt.Println("✅ Email sent successfully")
	}

	fmt.Println("✅ Program exited cleanly")
}
