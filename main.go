package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"math"
	"math/rand"
	"net"
	"net/http"
	"time"
	"net/smtp"
	"path/filepath"
	"fmt"
	"log"
	"os"
	"sync"
	"encoding/base64"
	"database/sql"
	"strings"
	// SQLite driver
	_ "github.com/mattn/go-sqlite3"

	// uTLS (will be used later for HTTP client)
	utls "github.com/refraction-networking/utls"
)
const MAX_RUNTIME_MINUTES = 355 // ⏱️ CHANGE THIS
var PREFIX_SEMAPHORE = make(chan struct{}, MAX_INFLIGHT)
const MAX_INFLIGHT = 20

var (
	START_TIME = time.Now()
	END_TIME   = START_TIME.Add(time.Duration(MAX_RUNTIME_MINUTES) * time.Minute)
)
var STOP_EVENT = make(chan struct{})

func initDB(dbName string) {
	if dbName == "" {
		dbName = "OUTPUT.db"
	}

	db, err := sql.Open("sqlite3", dbName)
	if err != nil {
		log.Fatalf("DB open error: %v", err)
	}
	defer db.Close()

	pragmas := []string{
		"PRAGMA journal_mode=WAL;",
		"PRAGMA synchronous=NORMAL;",
		"PRAGMA cache_size=-50000;",   // ~50MB memory
		"PRAGMA temp_store=MEMORY;",   // faster
		"PRAGMA locking_mode=EXCLUSIVE;",
	}

	for _, pragma := range pragmas {
		if _, err := db.Exec(pragma); err != nil {
			log.Fatalf("PRAGMA error (%s): %v", pragma, err)
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
	);
	`

	if _, err := db.Exec(createTable); err != nil {
		log.Fatalf("Table creation error: %v", err)
	}
}

var PREFIXES = []string{
	"X1",
}
var USER_AGENTS = []string{
	// Desktop browsers
	"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
	"Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
	"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Firefox/118.0",
	"Mozilla/5.0 (Windows NT 6.1; WOW64; rv:102.0) Gecko/20100101 Firefox/102.0",

	// Mobile browsers
	"Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1",
	"Mozilla/5.0 (Linux; Android 11; Pixel 4 XL) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Mobile Safari/537.36",
}
var SUFFIX_CHARS = []string{
	"0", "1", "2", "3", "4", "5", "6", "7", "8", "9",
	"A", "B", "C", "D", "E", "F", "G", "H", "J", "K", "L",
	"M", "N", "P", "Q", "R", "S", "T", "U", "V", "W",
	"X", "Y", "Z",
}
var MAX_PARALLEL_PREFIXES = len(PREFIXES)
const FAILED_CODES_FILE = "failed_403_codes.log"
var failedCodeMutex sync.Mutex
func saveFailedCode(workerID, code, reason string) {
	failedCodeMutex.Lock()
	defer failedCodeMutex.Unlock()

	f, err := os.OpenFile(
		FAILED_CODES_FILE,
		os.O_APPEND|os.O_CREATE|os.O_WRONLY,
		0644,
	)
	if err != nil {
		return // silent fail, same spirit as Python
	}
	defer f.Close()

	line := fmt.Sprintf(
		"%s | %s | %s | %s\n",
		time.Now().Format(time.RFC3339),
		workerID,
		code,
		reason,
	)

	_, _ = f.WriteString(line)
}
func newUTLSHttpClient() *http.Client {
	dialTLS := func(ctx context.Context, network, addr string) (net.Conn, error) {
		dialer := &net.Dialer{
			Timeout: 50 * time.Second,
		}

		rawConn, err := dialer.DialContext(ctx, network, addr)
		if err != nil {
			return nil, err
		}

		cfg := &utls.Config{
			ServerName:         addr[:len(addr)-len(":443")],
			NextProtos:         []string{"h2", "http/1.1"},
			InsecureSkipVerify: false,
		}

		uconn := utls.UClient(rawConn, cfg, utls.HelloChrome_Auto)
		if err := uconn.Handshake(); err != nil {
			return nil, err
		}

		return uconn, nil
	}

	tr := &http.Transport{
		DialTLSContext: dialTLS,
		ForceAttemptHTTP2: false,
		MaxIdleConns:       34,
		MaxIdleConnsPerHost: 34,
	}

	return &http.Client{
		Transport: tr,
		Timeout:   50 * time.Second,
	}
}
func fetchCode(localCode string, client *http.Client, sessionID string) string {
	payload := map[string]string{
		"Guid":    localCode,
		"Lng":     "en",
		"partner": "159",
	}

	body, _ := json.Marshal(payload)

	req, err := http.NewRequest(
		"POST",
		"https://1xbet.ng/service-api/LiveBet/Open/GetCoupon",
		bytes.NewBuffer(body),
	)
	if err != nil {
		return "ERROR_RETRY"
	}

	req.Header.Set("User-Agent", USER_AGENTS[rand.Intn(len(USER_AGENTS))])
	req.Header.Set("Accept", "application/json, text/plain, */*")
	req.Header.Set("Accept-Language", "en-CA,en;q=0.9")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Connection", "keep-alive")
	req.Header.Set("Cache-Control", "no-cache")
	req.Header.Set("Pragma", "no-cache")
	req.Header.Set("Referer", "https://1xbet.ng/en")
	req.Header.Set("Origin", "https://1xbet.ng")

	resp, err := client.Do(req)
	if err != nil {
		return "ERROR_RETRY"
	}
	defer resp.Body.Close()

	if resp.StatusCode == 403 {
		return "ERROR_403"
	}

	if resp.StatusCode != 200 {
		fmt.Println(resp.StatusCode)
		return "ERROR_RETRY"
	}

	contentType := resp.Header.Get("Content-Type")
	raw, _ := io.ReadAll(resp.Body)

	if !bytes.HasPrefix([]byte(contentType), []byte("application/json")) {
		return "ERROR_RETRY"
	}

	var response map[string]any
	if err := json.Unmarshal(raw, &response); err != nil {
		return "ERROR_RETRY"
	}

	success, ok := response["Success"].(bool)
	if !ok || !success {
		fmt.Println("INVALID")
		return "INVALID"
	}

	value := response["Value"].(map[string]any)
	events := value["Events"].([]any)

	if len(events) > 15 {
		fmt.Println("VALID")
		return "VALID"
	}

	var (
		eventsStatus []bool
		matchDates   []time.Time
		odds         []float64
		sports       []string
		teams        []string
		eventNames   []string
		scores       []string
		matchTimes   []string
	)

	today := time.Now().Truncate(24 * time.Hour)

	for _, ev := range events {
		e := ev.(map[string]any)

		eventsStatus = append(eventsStatus, e["Finish"].(bool))
		start := time.Unix(int64(e["Start"].(float64)), 0)
		matchDates = append(matchDates, start.Truncate(24*time.Hour))
		matchTimes = append(matchTimes, start.Format("15:04:05"))

		odd := e["Coef"].(float64)
		odds = append(odds, odd)

		sports = append(sports, e["SportNameEng"].(string))
		eventNames = append(eventNames, e["GroupName"].(string))
		scores = append(scores, e["MarketName"].(string))

		team := e["Opp1"].(string) + " vs " + e["Opp2"].(string)
		teams = append(teams, team)
	}

	totalOdd := odds[0]
	for i := 1; i < len(odds); i++ {
		totalOdd *= odds[i]
	}

	if totalOdd <= 80 || totalOdd >= 500 {
		return "VALID"
	}

	for _, s := range sports {
		if s != "Football" {
			return "VALID"
		}
	}

	for _, d := range matchDates {
		if !d.Equal(today) {
			return "VALID"
		}
	}

	for _, st := range eventsStatus {
		if st {
			return "VALID"
		}
	}

	initDB("OUTPUT.db")
	logCode(
		len(events),
		localCode,
		sessionID,
		strings.Join(teams, "|"),
		strings.Join(eventNames, "|"),
		strings.Join(scores, "|"),
		strings.Join(matchTimes, "|"),
		func() string {
			out := make([]string, len(odds))
			for i, o := range odds {
				out[i] = fmt.Sprintf("%f", o)
			}
			return strings.Join(out, "|")
		}(),
		fmt.Sprintf("%f", math.Round(totalOdd*100)/100),
		"NA",
	)

	return "VALID"
}
func fourthWorker(
	prefix string,
	fourthChar string,
	client *http.Client,
	workerID string,
) {
	fmt.Printf("[%s] 🚀 Worker %s started\n", prefix, fourthChar)

	for _, a := range SUFFIX_CHARS {

		// STOP_EVENT check (non-blocking)
		select {
		case <-STOP_EVENT:
			fmt.Printf("[%s] 🛑 Worker %s stopping (STOP_EVENT)\n", prefix, fourthChar)
			return
		default:
		}

		for _, b := range SUFFIX_CHARS {

			select {
			case <-STOP_EVENT:
				fmt.Printf("[%s] 🛑 Worker %s stopping (STOP_EVENT)\n", prefix, fourthChar)
				return
			default:
			}

			code := prefix + fourthChar + a + b

			for {
				result := fetchCode(code, client, workerID)

				// 🔴 CASE 1: HTTP 403 → SAVE + RETRY SAME CODE
				if result == "ERROR_403" {
					saveFailedCode(workerID, code, "403")
					fmt.Printf("[%s] 🔄 403 on %s, requesting TLS reset\n", workerID, code)
					continue
				}

				// 🟡 CASE 2: Retryable errors → retry SAME code
				if result == "ERROR_RETRY" || result == "ERROR_TIMEOUT" {
					continue
				}

				// 🟢 CASE 3: Normal response → move to next code
				break
			}
		}
	}

	fmt.Printf("[%s] ✅ Worker %s finished normally\n", prefix, fourthChar)
}
func processPrefix(prefix string) {
	// Acquire semaphore
	PREFIX_SEMAPHORE <- struct{}{}
	defer func() { <-PREFIX_SEMAPHORE }() // release on exit

	fmt.Printf("\n🔐 STARTING PREFIX %s\n", prefix)

	for {
		select {
		case <-STOP_EVENT:
			fmt.Printf("[%s] 🛑 STOP_EVENT set, exiting processPrefix\n", prefix)
			return
		default:
		}

		client := newUTLSHttpClient() // new TLS handshake each loop
		tasksDone := make(chan struct{})

		// Launch all fourthChar workers concurrently
		for _, fourth := range SUFFIX_CHARS {
			go func(fourthChar string) {
				fourthWorker(prefix, fourthChar, client, prefix+"-"+fourthChar)
				tasksDone <- struct{}{}
			}(fourth)
		}

		// Wait for all workers to finish
		for i := 0; i < len(SUFFIX_CHARS); i++ {
			<-tasksDone
		}

		// No 403 restart logic included for now
		break // normal completion
	}

	fmt.Printf("🏁 PREFIX %s COMPLETED\n\n", prefix)
}
func logCode(
	label int,
	code string,
	workerID string,
	teams string,
	events string,
	score string,
	timeStr string,
	odds string,
	totalOdds string,
	lastChange string,
	dbName ...string, // optional
) {
	dbFile := "OUTPUT.db"
	if len(dbName) > 0 {
		dbFile = dbName[0]
	}

	db, err := sql.Open("sqlite3", dbFile)
	if err != nil {
		log.Fatalf("DB open error: %v", err)
	}
	defer db.Close()

	stmt := `
	INSERT INTO codes (
		worker_id,
		label,
		code,
		teams,
		events,
		score,
		time,
		odds,
		total_odds,
		last_change
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`

	_, err = db.Exec(stmt,
		workerID,
		label,
		code,
		teams,
		events,
		score,
		timeStr,
		odds,
		totalOdds,
		lastChange,
	)
	if err != nil {
		log.Printf("[Worker %s] DB insert error: %v", workerID, err)
		return
	}

	fmt.Printf("[Worker %s] => %d: %s (%s)\n", workerID, label, code, teams)
}
func sendDBViaGmail(senderEmail, appPassword, recipientEmail, dbPath string) {
	if dbPath == "" {
		dbPath = "OUTPUT.db"
	}

	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		fmt.Println("📭 OUTPUT.db not found. No email sent.")
		return
	}

	data, err := os.ReadFile(dbPath)
	if err != nil {
		fmt.Printf("📭 Failed to read DB: %v\n", err)
		return
	}

	// Compose basic email with attachment in RFC822 MIME
	boundary := "SPORTYBET_BOUNDARY"
	subject := "SportyBet Script Output DB"
	filename := filepath.Base(dbPath)

	msg := fmt.Sprintf(
		"From: %s\r\nTo: %s\r\nSubject: %s\r\nMIME-Version: 1.0\r\nContent-Type: multipart/mixed; boundary=%s\r\n\r\n--%s\r\nContent-Type: text/plain; charset=utf-8\r\n\r\nAttached is the OUTPUT.db generated by the script.\r\n\r\n--%s\r\nContent-Type: application/octet-stream\r\nContent-Disposition: attachment; filename=\"%s\"\r\nContent-Transfer-Encoding: base64\r\n\r\n%s\r\n--%s--",
		senderEmail,
		recipientEmail,
		subject,
		boundary,
		boundary,
		boundary,
		filename,
		base64.StdEncoding.EncodeToString(data),
		boundary,
	)

	err = smtp.SendMail(
		"smtp.gmail.com:587",
		smtp.PlainAuth("", senderEmail, appPassword, "smtp.gmail.com"),
		senderEmail,
		[]string{recipientEmail},
		[]byte(msg),
	)
	if err != nil {
		fmt.Printf("📧 Failed to send email: %v\n", err)
		return
	}

	fmt.Println("📧 OUTPUT.db sent successfully via Gmail.")
}
func runtimeWatchdog(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if time.Now().After(END_TIME) {
				fmt.Println("⏰ MAX EXECUTION TIME REACHED — STOPPING SCRIPT")
				close(STOP_EVENT) // signal stop
				return
			}
		case <-ctx.Done():
			return
		}
	}
}
func mainAsync() {
	fmt.Println("STARTING PREFIX ENGINE")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start watchdog
	go runtimeWatchdog(ctx)

	// Start all prefixes
	var wg sync.WaitGroup
	for _, prefix := range PREFIXES {
		wg.Add(1)
		go func(p string) {
			defer wg.Done()
			processPrefix(p) // you already have this function
		}(prefix)
	}

	// Wait until STOP_EVENT is closed
	<-STOP_EVENT
	fmt.Println("🛑 Cancelling remaining tasks...")

	// Wait for all prefix goroutines to finish gracefully
	wg.Wait()
}
func main() {
	log.SetOutput(io.Discard)
	// 🔒 Read credentials safely
	sender := os.Getenv("GMAIL_SENDER")
	password := os.Getenv("GMAIL_APP_PASSWORD")
	receiver := os.Getenv("GMAIL_RECEIVER")

	// ✅ Python finally-equivalent
	defer func() {
		fmt.Println("📤 Program exiting — attempting to send OUTPUT.db")

		if sender == "" || password == "" || receiver == "" {
			fmt.Println("⚠️ Gmail env vars missing — skipping email")
			return
		}

		sendDBViaGmail(
			sender,
			password,
			receiver,
			"OUTPUT.db",
		)
	}()

	// 🛡️ Panic safety (keep this)
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("💥 Recovered from panic:", r)
		}
	}()

	// 🚀 Run main engine
	mainAsync()
}
