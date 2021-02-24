package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/bits"
	"net/http"
	"os"
	"sort"
	"sync"
	"time"
)

type Account struct {
	Email    string `json:"email"`
	Password string `json:"password"`
	Flags    int    `json:"flags"`
}

func NewAccount(email, password string, flags int) *Account {
	return &Account{Email: email, Password: password, Flags: flags}
}

type waiter struct {
	priority int
	wakeUp   chan struct{}
}

type stats struct {
	waiting        int
	done           int
	maxWaiters     int
	timeByPriority map[int][]time.Duration
	timeouts       int
	returns        int
	rentTimes      []time.Duration
	rentAcquired   map[string]time.Time
}

func (s *stats) addDuration(priority int, dur time.Duration) {
	s.timeByPriority[priority] = append(s.timeByPriority[priority], dur)
	if len(s.timeByPriority[priority]) > 10000 {
		s.timeByPriority[priority] = s.timeByPriority[priority][1:]
	}
}

func (s *stats) rentStarted(email string) {
	s.rentAcquired[email] = time.Now()
}

func (s *stats) rentFinished(email string) {
	v, ok := s.rentAcquired[email]
	if !ok {
		return
	}

	dur := time.Since(v)
	s.rentTimes = append(s.rentTimes, dur)
	if len(s.rentTimes) > 10000 {
		s.rentTimes = s.rentTimes[1:]
	}
}

func (s *stats) addWaiting() {
	s.waiting++
}

func (s *stats) removeWaiting() {
	s.waiting--
}

func (s *stats) markDone() {
	s.done++
}

func (s *stats) returnedByTimer() {
	s.timeouts++
}

func (s *stats) returnedByUser() {
	s.returns++
}

func (s *stats) AvgWaitBy(priority int) time.Duration {
	durations, ok := s.timeByPriority[priority]
	if !ok || len(durations) == 0 {
		return time.Duration(0)
	}

	sum := time.Duration(0)
	for _, dur := range durations {
		sum += dur
	}

	return time.Duration(int(sum) / len(durations))
}

func (s *stats) AvgWait() time.Duration {
	sum := time.Duration(0)
	i := 0
	for _, durations := range s.timeByPriority {
		for _, dur := range durations {
			sum += dur
			i++
		}
	}

	if i == 0 {
		return time.Duration(0)
	}

	return time.Duration(int(sum) / i)
}

func (s *stats) AvgRentTime() time.Duration {
	sum := time.Duration(0)

	if len(s.rentTimes) == 0 {
		return sum
	}

	for _, dur := range s.rentTimes {
		sum += dur
	}

	return time.Duration(int(sum) / len(s.rentTimes))
}

func (s *stats) Pretty() string {
	return fmt.Sprintf(`Done: %d
Waiting: %d
Max waiters: %d
Manual Returns: %d
Force Returns (by timer): %d
Avg wait: %s
Avg rent time: %s`,
		s.done, s.waiting, s.maxWaiters, s.returns, s.timeouts, s.AvgWait(), s.AvgRentTime(),
	)
}

type AccountList struct {
	free         []*Account
	accounts     map[string]*Account
	waiters      []*waiter
	lock         *sync.Mutex
	stats        *stats
	rentTimeout  int
	rentChannels map[string]chan struct{}
}

func NewAccountList(rentTimeout int) *AccountList {
	return &AccountList{
		free:     make([]*Account, 0),
		accounts: make(map[string]*Account),
		waiters:  make([]*waiter, 0),
		lock:     &sync.Mutex{},
		stats: &stats{
			waiting:        0,
			done:           0,
			maxWaiters:     0,
			timeByPriority: make(map[int][]time.Duration),
			timeouts:       0,
			returns:        0,
			rentTimes:      make([]time.Duration, 0),
			rentAcquired:   make(map[string]time.Time),
		},
		rentTimeout:  rentTimeout,
		rentChannels: make(map[string]chan struct{}),
	}
}

func (a *AccountList) Load() {
	b, err := os.ReadFile("cfg.json")
	if err != nil {
		log.Fatal("Cannot read cfg.json")
	}

	accs := make([]*Account, 0)

	if err := json.Unmarshal(b, &accs); err != nil {
		log.Fatal("Error decoding JSON file")
	}

	if len(accs) == 0 {
		log.Fatal("File does not contain any accounts")
	}

	a.free = accs
	for _, acc := range accs {
		a.accounts[acc.Email] = acc
	}
}

func (a *AccountList) insertWaiter(w *waiter) {
	i := sort.Search(len(a.waiters), func(i int) bool {
		return a.waiters[i].priority <= w.priority
	})

	a.lock.Lock()
	a.waiters = append(a.waiters, nil)
	copy(a.waiters[i+1:], a.waiters[i:])
	a.waiters[i] = w
	if len(a.waiters) > a.stats.maxWaiters {
		a.stats.maxWaiters = len(a.waiters)
	}
	a.lock.Unlock()
}

func (a *AccountList) dropWaiter(w *waiter) {
	index := -1
	for i, v := range a.waiters {
		if v.wakeUp == w.wakeUp {
			index = i
		}
	}

	copy(a.waiters[index:], a.waiters[index+1:])
	a.waiters[len(a.waiters)-1] = nil
	a.waiters = a.waiters[:len(a.waiters)-1]
}

func (a *AccountList) getByPred(priority int, pred func(acc *Account) bool) *Account {
	w := &waiter{
		priority: priority,
		wakeUp:   make(chan struct{}),
	}
	a.stats.addWaiting()
	start := time.Now()

	a.insertWaiter(w)
	for {
		a.lock.Lock()
		for i, acc := range a.free {
			if pred(acc) {
				copy(a.free[i:], a.free[i+1:])
				a.free = a.free[:len(a.free)-1]
				a.dropWaiter(w)
				duration := time.Since(start)
				a.stats.addDuration(priority, duration)
				a.stats.markDone()
				a.stats.removeWaiting()
				a.rentChannels[acc.Email] = make(chan struct{})
				a.stats.rentStarted(acc.Email)
				go a.returnBack(acc)
				a.lock.Unlock()
				return acc
			}
		}
		a.lock.Unlock()

		<-w.wakeUp
	}
}

func (a *AccountList) GetAny() *Account {
	return a.getByPred(0, func(acc *Account) bool {
		return true
	})
}

func (a *AccountList) GetByEmail(email string) *Account {
	return a.getByPred(10, func(acc *Account) bool {
		return acc.Email == email
	})
}

func (a *AccountList) GetByFlags(flags int) *Account {
	priority := bits.OnesCount(uint(flags))
	return a.getByPred(priority, func(acc *Account) bool {
		return acc.Flags&flags == flags
	})
}

func (a *AccountList) Put(acc *Account) {
	a.lock.Lock()
	defer a.lock.Unlock()

	for _, v := range a.free {
		if v.Email == acc.Email {
			return
		}
	}

	a.stats.rentFinished(acc.Email)

	rentChan, ok := a.rentChannels[acc.Email]
	if ok {
		select {
		case rentChan <- struct{}{}:
		default:
		}
	}

	a.free = append(a.free, acc)
	for _, w := range a.waiters {
		w.wakeUp <- struct{}{}
		time.Sleep(3 * time.Millisecond)
	}
}

func (a *AccountList) PutBack(email string) bool {
	acc, ok := a.accounts[email]
	if !ok {
		return false
	}

	a.Put(acc)
	return true
}

func (a *AccountList) returnBack(acc *Account) {
	log.Println("will return", acc, "back in", a.rentTimeout, "seconds")
	timer := time.NewTimer(time.Second * time.Duration(a.rentTimeout))
	rentChan, ok := a.rentChannels[acc.Email]
	if !ok {
		log.Fatal("WTF")
	}
	select {
	case <-timer.C:
		a.stats.returnedByTimer()
		a.Put(acc)
	case <-rentChan:
		a.stats.returnedByUser()
	}
}

type GetRequest struct {
	Email string `json:"email"`
	Flags int    `json:"flags"`
}

type PutRequest struct {
	Email string `json:"email"`
}

func main() {
	timeout := 0
	flag.IntVar(&timeout, "timeout", 10*60, "Timeout to get the account back (in seconds)")
	flag.Parse()

	list := NewAccountList(timeout)
	list.Load()

	http.HandleFunc("/stats", func(writer http.ResponseWriter, request *http.Request) {
		_, err := writer.Write([]byte(list.stats.Pretty()))
		if err != nil {
			log.Println("Error in /stats", err)
		}
	})

	http.HandleFunc("/get", func(writer http.ResponseWriter, request *http.Request) {
		req := &GetRequest{}
		if err := json.NewDecoder(request.Body).Decode(&req); err != nil {
			log.Println(err)
			writer.WriteHeader(400)
			return
		}

		acc := (*Account)(nil)
		if req.Email != "" {
			acc = list.GetByEmail(req.Email)
		} else {
			acc = list.GetByFlags(req.Flags)
		}

		b, err := json.Marshal(acc)
		if err != nil {
			writer.WriteHeader(500)
		}
		_, err = writer.Write(b)
		if err != nil {
			log.Println("Error writing to socket", err)
		}
	})

	http.HandleFunc("/put", func(writer http.ResponseWriter, request *http.Request) {
		req := &PutRequest{}
		if err := json.NewDecoder(request.Body).Decode(&req); err != nil {
			log.Println(err)
			writer.WriteHeader(400)
			return
		}
		ok := list.PutBack(req.Email)
		if !ok {
			writer.WriteHeader(422)
			return
		}
		writer.Write([]byte("ok"))
	})

	log.Println("Listening @ 8000 port")
	log.Fatal(http.ListenAndServe(":8000", nil))
}
