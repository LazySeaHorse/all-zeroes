package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
)

// ---- config ----------------------------------------------------------------

type config struct {
	token      string
	allowed    map[int64]bool
	backendURL string
	apiKey     string
	ncURL      string
	ncToken    string
}

func loadConfig() config {
	c := config{
		token:      mustEnv("TELEGRAM_BOT_TOKEN"),
		backendURL: strings.TrimRight(mustEnv("BACKEND_URL"), "/"),
		apiKey:     mustEnv("BACKEND_API_KEY"),
		ncURL:      mustEnv("NEXTCLOUD_URL"),
		ncToken:    mustEnv("NEXTCLOUD_TOKEN"),
		allowed:    map[int64]bool{},
	}
	for _, s := range strings.Split(mustEnv("TELEGRAM_ALLOWED_CHAT_IDS"), ",") {
		s = strings.TrimSpace(s)
		if s == "" {
			continue
		}
		id, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			log.Fatalf("invalid chat id %q: %v", s, err)
		}
		c.allowed[id] = true
	}
	return c
}

func mustEnv(k string) string {
	v := os.Getenv(k)
	if v == "" {
		log.Fatalf("required env var %s is not set", k)
	}
	return v
}

// ---- backend client --------------------------------------------------------

type jobResp struct {
	ID           string     `json:"id"`
	Filename     string     `json:"filename"`
	Status       string     `json:"status"`
	Stage        string     `json:"stage"`
	DeliverNow   bool       `json:"deliver_now"`
	Error        *string    `json:"error"`
	ChunksTotal  int        `json:"chunks_total"`
	ChunksDone   int        `json:"chunks_done"`
	CurrentChunk *chunkResp `json:"current_chunk"`
}

type chunkResp struct {
	Idx  int    `json:"idx"`
	URL  string `json:"url"`
	Size int64  `json:"size"`
}

type backendClient struct {
	cfg  config
	http *http.Client
}

func newBackendClient(c config) *backendClient {
	return &backendClient{cfg: c, http: &http.Client{Timeout: 15 * time.Second}}
}

func (bc *backendClient) do(method, path string, body any) (*http.Response, error) {
	var r io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		r = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, bc.cfg.backendURL+path, r)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+bc.cfg.apiKey)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	return bc.http.Do(req)
}

func (bc *backendClient) listJobs() ([]jobResp, error) {
	resp, err := bc.do("GET", "/api/jobs", nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var out struct {
		Jobs []jobResp `json:"jobs"`
	}
	return out.Jobs, json.NewDecoder(resp.Body).Decode(&out)
}

func (bc *backendClient) submitJob(url, filename, stage string, noChunk, deliverNow bool) (*jobResp, error) {
	dn := deliverNow
	payload := map[string]any{
		"url":             url,
		"filename":        filename,
		"stage":           stage,
		"no_chunk":        noChunk,
		"deliver_now":     &dn,
		"nextcloud_url":   bc.cfg.ncURL,
		"nextcloud_token": bc.cfg.ncToken,
	}
	resp, err := bc.do("POST", "/api/jobs", payload)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		b, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("backend %d: %s", resp.StatusCode, strings.TrimSpace(string(b)))
	}
	var j jobResp
	return &j, json.NewDecoder(resp.Body).Decode(&j)
}

func (bc *backendClient) ackChunk(jobID string, idx int) error {
	resp, err := bc.do("POST", "/api/jobs/"+jobID+"/chunks/done", map[string]int{"idx": idx})
	if err != nil {
		return err
	}
	resp.Body.Close()
	if resp.StatusCode != 204 && resp.StatusCode >= 300 {
		return fmt.Errorf("ack returned %d", resp.StatusCode)
	}
	return nil
}

func (bc *backendClient) deleteJob(jobID string) error {
	resp, err := bc.do("DELETE", "/api/jobs/"+jobID, nil)
	if err != nil {
		return err
	}
	resp.Body.Close()
	return nil
}

func (bc *backendClient) deliverJob(jobID string) error {
	resp, err := bc.do("POST", "/api/jobs/"+jobID+"/deliver", nil)
	if err != nil {
		return err
	}
	resp.Body.Close()
	if resp.StatusCode != 202 && resp.StatusCode >= 300 {
		return fmt.Errorf("deliver returned %d", resp.StatusCode)
	}
	return nil
}

// ---- bot state -------------------------------------------------------------

type pendingSub struct {
	sourceURL  string
	filename   string
	msgID      int
	stage      string
	noChunk    bool
	deliverNow bool
}

type trackedJob struct {
	lastStatus   string
	lastChunkIdx int // -1 = no chunk message sent yet for this job
}

type bot struct {
	api     *tgbotapi.BotAPI
	cfg     config
	bc      *backendClient
	mu      sync.Mutex
	pending map[int64]*pendingSub  // chatID → pending submission
	tracked map[string]*trackedJob // jobID → polling state
}

// ---- keyboards -------------------------------------------------------------

func (b *bot) optionsKB(p *pendingSub) tgbotapi.InlineKeyboardMarkup {
	vps, stream, gdrive := "VPS", "Stream", "GDrive"
	switch p.stage {
	case "vps":
		vps = "✓ VPS"
	case "stream":
		stream = "✓ Stream"
	case "gdrive":
		gdrive = "✓ GDrive"
	}
	noChunk := "No-chunk: OFF"
	if p.noChunk {
		noChunk = "No-chunk: ON ✓"
	}
	deliver := "Deliver now: ON ✓"
	if !p.deliverNow {
		deliver = "Deliver now: OFF"
	}
	return tgbotapi.NewInlineKeyboardMarkup(
		tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData(vps, "o:stage:vps"),
			tgbotapi.NewInlineKeyboardButtonData(stream, "o:stage:stream"),
			tgbotapi.NewInlineKeyboardButtonData(gdrive, "o:stage:gdrive"),
		),
		tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData(noChunk, "o:nochunk"),
			tgbotapi.NewInlineKeyboardButtonData(deliver, "o:delivernow"),
		),
		tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData("🚀 Submit", "o:submit"),
		),
	)
}

func chunkKB(jobID string, idx int) tgbotapi.InlineKeyboardMarkup {
	return tgbotapi.NewInlineKeyboardMarkup(
		tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData("✅ Done", fmt.Sprintf("done:%s:%d", jobID, idx)),
			tgbotapi.NewInlineKeyboardButtonData("❌ Cancel", "cancel:"+jobID),
		),
	)
}

func stagedKB(jobID string) tgbotapi.InlineKeyboardMarkup {
	return tgbotapi.NewInlineKeyboardMarkup(
		tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData("🚀 Deliver", "deliver:"+jobID),
			tgbotapi.NewInlineKeyboardButtonData("❌ Cancel", "cancel:"+jobID),
		),
	)
}

// ---- send helpers ----------------------------------------------------------

func (b *bot) send(chatID int64, text string) {
	msg := tgbotapi.NewMessage(chatID, text)
	msg.ParseMode = "Markdown"
	msg.DisableWebPagePreview = true
	b.api.Send(msg) //nolint:errcheck
}

func (b *bot) sendWithKB(chatID int64, text string, kb tgbotapi.InlineKeyboardMarkup) {
	msg := tgbotapi.NewMessage(chatID, text)
	msg.ParseMode = "Markdown"
	msg.DisableWebPagePreview = true
	msg.ReplyMarkup = kb
	b.api.Send(msg) //nolint:errcheck
}

func (b *bot) removeKB(chatID int64, msgID int) {
	b.api.Send(tgbotapi.NewEditMessageReplyMarkup(chatID, msgID, tgbotapi.InlineKeyboardMarkup{})) //nolint:errcheck
}

// ---- message handling ------------------------------------------------------

func (b *bot) handleMessage(msg *tgbotapi.Message) {
	chatID := msg.Chat.ID
	if !b.cfg.allowed[chatID] {
		return
	}

	if msg.IsCommand() {
		switch msg.Command() {
		case "start", "list", "status":
			b.cmdList(chatID)
		case "cancel":
			b.cmdCancel(chatID, msg.CommandArguments())
		case "deliver":
			b.cmdDeliver(chatID, msg.CommandArguments())
		default:
			b.send(chatID, "Commands: /list · /cancel <id> · /deliver <id>")
		}
		return
	}

	var sourceURL, filename string

	if msg.Document != nil {
		fi, err := b.api.GetFile(tgbotapi.FileConfig{FileID: msg.Document.FileID})
		if err != nil {
			b.send(chatID, "❌ Could not get file info: "+err.Error())
			return
		}
		sourceURL = fi.Link(b.cfg.token)
		filename = msg.Document.FileName
	} else if msg.Text != "" {
		text := strings.TrimSpace(msg.Text)
		if !strings.HasPrefix(text, "http://") && !strings.HasPrefix(text, "https://") {
			b.send(chatID, "Send a URL or a file.")
			return
		}
		sourceURL = text
	} else {
		b.send(chatID, "Send a URL or a file.")
		return
	}

	p := &pendingSub{
		sourceURL:  sourceURL,
		filename:   filename,
		stage:      "vps",
		noChunk:    false,
		deliverNow: true,
	}

	label := filename
	if label == "" {
		label = truncate(sourceURL, 50)
	}

	m := tgbotapi.NewMessage(chatID, "⚙️ *Options* for `"+escMD(label)+"`:")
	m.ParseMode = "Markdown"
	m.DisableWebPagePreview = true
	m.ReplyMarkup = b.optionsKB(p)
	sent, err := b.api.Send(m)
	if err != nil {
		b.send(chatID, "❌ "+err.Error())
		return
	}

	p.msgID = sent.MessageID
	b.mu.Lock()
	b.pending[chatID] = p
	b.mu.Unlock()
}

// ---- callback handling -----------------------------------------------------

func (b *bot) handleCallback(cb *tgbotapi.CallbackQuery) {
	chatID := cb.Message.Chat.ID
	if !b.cfg.allowed[chatID] {
		return
	}
	b.api.Request(tgbotapi.NewCallback(cb.ID, "")) //nolint:errcheck

	data := cb.Data
	switch {
	case strings.HasPrefix(data, "o:"):
		b.handleOption(chatID, cb.Message.MessageID, strings.TrimPrefix(data, "o:"))
	case strings.HasPrefix(data, "done:"):
		parts := strings.SplitN(strings.TrimPrefix(data, "done:"), ":", 2)
		if len(parts) == 2 {
			idx, _ := strconv.Atoi(parts[1])
			b.handleDone(chatID, parts[0], idx, cb.Message.MessageID)
		}
	case strings.HasPrefix(data, "cancel:"):
		b.handleCancelBtn(chatID, strings.TrimPrefix(data, "cancel:"), cb.Message.MessageID)
	case strings.HasPrefix(data, "deliver:"):
		b.handleDeliverBtn(chatID, strings.TrimPrefix(data, "deliver:"), cb.Message.MessageID)
	}
}

func (b *bot) handleOption(chatID int64, msgID int, action string) {
	b.mu.Lock()
	p := b.pending[chatID]
	if p == nil || p.msgID != msgID {
		b.mu.Unlock()
		return
	}

	if action == "submit" {
		delete(b.pending, chatID)
		b.mu.Unlock()
		b.submitPending(chatID, msgID, p)
		return
	}

	switch action {
	case "stage:vps":
		p.stage = "vps"
	case "stage:stream":
		p.stage = "stream"
	case "stage:gdrive":
		p.stage = "gdrive"
	case "nochunk":
		p.noChunk = !p.noChunk
	case "delivernow":
		p.deliverNow = !p.deliverNow
	}
	kb := b.optionsKB(p)
	b.mu.Unlock()

	b.api.Send(tgbotapi.NewEditMessageReplyMarkup(chatID, msgID, kb)) //nolint:errcheck
}

func (b *bot) submitPending(chatID int64, msgID int, p *pendingSub) {
	b.removeKB(chatID, msgID)

	j, err := b.bc.submitJob(p.sourceURL, p.filename, p.stage, p.noChunk, p.deliverNow)
	if err != nil {
		b.send(chatID, "❌ Submit failed: "+err.Error())
		return
	}

	b.mu.Lock()
	b.tracked[j.ID] = &trackedJob{lastStatus: j.Status, lastChunkIdx: -1}
	b.mu.Unlock()

	b.send(chatID, fmt.Sprintf("✅ Queued `%s` (`%s`)", escMD(j.ID[:8]), escMD(j.Filename)))
}

func (b *bot) handleDone(chatID int64, jobID string, idx, msgID int) {
	if err := b.bc.ackChunk(jobID, idx); err != nil {
		b.send(chatID, "❌ Ack failed: "+err.Error())
		return
	}
	b.removeKB(chatID, msgID)
}

func (b *bot) handleCancelBtn(chatID int64, jobID string, msgID int) {
	if err := b.bc.deleteJob(jobID); err != nil {
		b.send(chatID, "❌ Cancel failed: "+err.Error())
		return
	}
	b.removeKB(chatID, msgID)
	b.send(chatID, "🚫 Canceled.")
}

func (b *bot) handleDeliverBtn(chatID int64, jobID string, msgID int) {
	if err := b.bc.deliverJob(jobID); err != nil {
		b.send(chatID, "❌ Deliver failed: "+err.Error())
		return
	}
	b.removeKB(chatID, msgID)
}

// ---- commands --------------------------------------------------------------

func (b *bot) cmdList(chatID int64) {
	jobs, err := b.bc.listJobs()
	if err != nil {
		b.send(chatID, "❌ "+err.Error())
		return
	}
	if len(jobs) == 0 {
		b.send(chatID, "No jobs.")
		return
	}
	var sb strings.Builder
	for _, j := range jobs {
		sb.WriteString(statusIcon(j.Status))
		sb.WriteString(" `")
		sb.WriteString(j.ID[:8])
		sb.WriteString("` *")
		sb.WriteString(escMD(j.Filename))
		sb.WriteString("*\n   ")
		sb.WriteString(j.Status)
		if j.ChunksTotal > 0 {
			sb.WriteString(fmt.Sprintf(" · %d/%d chunks", j.ChunksDone, j.ChunksTotal))
		}
		sb.WriteString("\n\n")
	}
	b.send(chatID, sb.String())
}

func (b *bot) cmdCancel(chatID int64, args string) {
	prefix := strings.TrimSpace(args)
	if prefix == "" {
		b.send(chatID, "Usage: /cancel <job-id-prefix>")
		return
	}
	j, ok := b.findJob(chatID, prefix)
	if !ok {
		return
	}
	if err := b.bc.deleteJob(j.ID); err != nil {
		b.send(chatID, "❌ Cancel failed: "+err.Error())
		return
	}
	b.send(chatID, "🚫 Canceled `"+escMD(j.ID[:8])+"`")
}

func (b *bot) cmdDeliver(chatID int64, args string) {
	prefix := strings.TrimSpace(args)
	if prefix == "" {
		b.send(chatID, "Usage: /deliver <job-id-prefix>")
		return
	}
	j, ok := b.findJob(chatID, prefix)
	if !ok {
		return
	}
	if err := b.bc.deliverJob(j.ID); err != nil {
		b.send(chatID, "❌ Deliver failed: "+err.Error())
		return
	}
	b.send(chatID, "🚀 Delivery started for `"+escMD(j.ID[:8])+"`")
}

func (b *bot) findJob(chatID int64, prefix string) (jobResp, bool) {
	jobs, err := b.bc.listJobs()
	if err != nil {
		b.send(chatID, "❌ "+err.Error())
		return jobResp{}, false
	}
	var matches []jobResp
	for _, j := range jobs {
		if strings.HasPrefix(j.ID, prefix) {
			matches = append(matches, j)
		}
	}
	switch len(matches) {
	case 0:
		b.send(chatID, "No job matching `"+escMD(prefix)+"`")
		return jobResp{}, false
	case 1:
		return matches[0], true
	default:
		b.send(chatID, "Ambiguous — matches multiple jobs, be more specific.")
		return jobResp{}, false
	}
}

// ---- polling loop ----------------------------------------------------------

func (b *bot) startPolling() {
	for {
		time.Sleep(4 * time.Second)
		jobs, err := b.bc.listJobs()
		if err != nil {
			log.Printf("poll error: %v", err)
			continue
		}

		type note struct {
			chatID int64
			fn     func()
		}
		var notes []note

		b.mu.Lock()
		for _, j := range jobs {
			j := j
			t, known := b.tracked[j.ID]
			if !known {
				t = &trackedJob{lastStatus: j.Status, lastChunkIdx: -1}
				b.tracked[j.ID] = t
				// Bot (re)started mid-delivery: resend chunk message so user has the link.
				if j.Status == "delivering" && j.CurrentChunk != nil {
					t.lastChunkIdx = j.CurrentChunk.Idx
					for chatID := range b.cfg.allowed {
						chatID, jj := chatID, j
						notes = append(notes, note{chatID, func() { b.sendChunkMsg(chatID, &jj) }})
					}
				}
				continue
			}

			// New chunk ready.
			if j.Status == "delivering" && j.CurrentChunk != nil && j.CurrentChunk.Idx != t.lastChunkIdx {
				t.lastChunkIdx = j.CurrentChunk.Idx
				for chatID := range b.cfg.allowed {
					chatID, jj := chatID, j
					notes = append(notes, note{chatID, func() { b.sendChunkMsg(chatID, &jj) }})
				}
			}

			// Status transition.
			if j.Status != t.lastStatus {
				t.lastStatus = j.Status
				for chatID := range b.cfg.allowed {
					chatID, jj := chatID, j
					switch j.Status {
					case "staged":
						if !j.DeliverNow {
							notes = append(notes, note{chatID, func() {
								b.sendWithKB(chatID,
									fmt.Sprintf("📦 *%s* staged — tap to deliver.", escMD(jj.Filename)),
									stagedKB(jj.ID))
							}})
						}
					case "done":
						notes = append(notes, note{chatID, func() {
							b.send(chatID, fmt.Sprintf("✅ *Done!* `%s`", escMD(jj.Filename)))
						}})
					case "failed":
						notes = append(notes, note{chatID, func() {
							errStr := ""
							if jj.Error != nil {
								errStr = ": " + *jj.Error
							}
							b.send(chatID, fmt.Sprintf("❌ *Failed* `%s`%s", escMD(jj.Filename), escMD(errStr)))
						}})
					}
				}
			}
		}
		b.mu.Unlock()

		for _, n := range notes {
			n.fn()
		}
	}
}

func (b *bot) sendChunkMsg(chatID int64, j *jobResp) {
	c := j.CurrentChunk
	text := fmt.Sprintf(
		"📦 *%s*\nChunk %d of %d · %s\n\n[⬇️ Download](%s)",
		escMD(j.Filename),
		c.Idx+1, j.ChunksTotal,
		fmtBytes(c.Size),
		c.URL,
	)
	msg := tgbotapi.NewMessage(chatID, text)
	msg.ParseMode = "Markdown"
	msg.ReplyMarkup = chunkKB(j.ID, c.Idx)
	b.api.Send(msg) //nolint:errcheck
}

// ---- helpers ---------------------------------------------------------------

func statusIcon(s string) string {
	switch s {
	case "queued":
		return "⏳"
	case "acquiring":
		return "⬇️"
	case "staged":
		return "📦"
	case "delivering":
		return "🚀"
	case "done":
		return "✅"
	case "failed":
		return "❌"
	case "canceled":
		return "🚫"
	default:
		return "❓"
	}
}

func fmtBytes(n int64) string {
	switch {
	case n >= 1<<30:
		return fmt.Sprintf("%.1f GB", float64(n)/float64(1<<30))
	case n >= 1<<20:
		return fmt.Sprintf("%.1f MB", float64(n)/float64(1<<20))
	case n >= 1<<10:
		return fmt.Sprintf("%.1f KB", float64(n)/float64(1<<10))
	default:
		return fmt.Sprintf("%d B", n)
	}
}

// escMD escapes Telegram legacy Markdown special characters.
func escMD(s string) string {
	var b strings.Builder
	for _, c := range s {
		if c == '*' || c == '_' || c == '`' || c == '[' {
			b.WriteRune('\\')
		}
		b.WriteRune(c)
	}
	return b.String()
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n-3] + "..."
}

// ---- main ------------------------------------------------------------------

func main() {
	c := loadConfig()

	api, err := tgbotapi.NewBotAPI(c.token)
	if err != nil {
		log.Fatalf("telegram: %v", err)
	}
	log.Printf("bot @%s started", api.Self.UserName)

	b := &bot{
		api:     api,
		cfg:     c,
		bc:      newBackendClient(c),
		pending: make(map[int64]*pendingSub),
		tracked: make(map[string]*trackedJob),
	}

	go b.startPolling()

	u := tgbotapi.NewUpdate(0)
	u.Timeout = 60
	for update := range api.GetUpdatesChan(u) {
		if update.Message != nil {
			go b.handleMessage(update.Message)
		}
		if update.CallbackQuery != nil {
			go b.handleCallback(update.CallbackQuery)
		}
	}
}
