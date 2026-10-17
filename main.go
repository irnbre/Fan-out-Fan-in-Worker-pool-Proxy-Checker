package main

import (
	"bufio"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

type Proxy struct {
	ip       string
	port     string
	login    string
	password string
	Latency  time.Duration
	IsValid  bool
	err      string
}

const (
	Green  = "\033[32m"
	Yellow = "\033[33m"
	Red    = "\033[31m"
	Reset  = "\033[0m"
)

func checkProxy(proxy Proxy, URL string) Proxy {
	proxyURL := &url.URL{
		Scheme: "socks5",
		Host:   proxy.ip + ":" + proxy.port,
		User:   url.UserPassword(proxy.login, proxy.password),
	}
	transport := &http.Transport{
		Proxy: http.ProxyURL(proxyURL),
	}
	defer transport.CloseIdleConnections()

	client := &http.Client{
		Transport: transport,
		Timeout:   4 * time.Second,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	start := time.Now()
	resp, err := client.Head(URL)
	proxy.Latency = time.Since(start)
	if err != nil {
		proxy.IsValid = false
		proxy.err = err.Error()
		return proxy
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 200 && resp.StatusCode < 400 {
		proxy.IsValid = true
	} else {
		proxy.IsValid = false
		proxy.err = resp.Status

	}
	return proxy
}

func jobber(jobber chan<- Proxy, sl []Proxy) {
	for _, proxy := range sl {
		jobber <- proxy
	}
	close(jobber)
}

func workers(jobber <-chan Proxy, results chan<- Proxy, wg *sync.WaitGroup, URL string) {
	defer wg.Done()
	for proxy := range jobber {
		result := checkProxy(proxy, URL)
		results <- result
	}
}

func main() {

	var URL string
	var col_workers int
	file, err := os.Open("proxies.txt")
	defer file.Close()
	if err != nil {
		fmt.Println("Ошибка открытия файла")
		return
	}
	reader := bufio.NewReader(file)
	fmt.Println("Введите кол-во потоков")
	fmt.Scan(&col_workers)
	if col_workers <= 0 {
		fmt.Println("Некорректное кол-во воркеров")
		return
	}
	fmt.Println("Введите URL в формате: https://google.com")
	fmt.Scan(&URL)
	sl := []Proxy{}
	var proxy string
	for {
		_, err := fmt.Fscan(reader, &proxy)
		if err == io.EOF {
			break
		}
		if err != nil {
			fmt.Println("Ошибка чтения")
			return
		}
		part := strings.Split(proxy, ":")
		if len(part) != 4 {
			fmt.Println("Неверный формат прокси")
			continue
		}
		sl = append(sl, Proxy{ip: part[0], port: part[1], login: part[2], password: part[3]})

	}
	wg := sync.WaitGroup{}
	job := make(chan Proxy)
	result := make(chan Proxy)
	go jobber(job, sl)
	for i := 0; i < col_workers; i++ {
		wg.Add(1)
		go workers(job, result, &wg, URL)
	}
	go func() {
		wg.Wait()
		close(result)
	}()
	counter := 0
	valid_counter := 0
	valid_sl := []Proxy{}
	for proxy := range result {
		counter += 1
		if proxy.IsValid == true {
			valid_counter++
			if proxy.Latency.Milliseconds() < 600 {
				fmt.Printf(Green+"[%d][ALIVE] %s:%s:%s:%s %dms"+Reset+"\n", counter, proxy.ip, proxy.port, proxy.login, proxy.password, proxy.Latency.Milliseconds())
				valid_sl = append(valid_sl, proxy)
			} else {
				fmt.Printf(Yellow+"[%d][ALIVE] %s:%s:%s:%s %dms"+Reset+"\n", counter, proxy.ip, proxy.port, proxy.login, proxy.password, proxy.Latency.Milliseconds())
				valid_sl = append(valid_sl, proxy)
			}

		} else {
			fmt.Printf(Red+"[%d][DEAD] %s:%s:%s:%s ERROR:%s"+Reset+"\n", counter, proxy.ip, proxy.port, proxy.login, proxy.password, proxy.err)
		}
	}
	fmt.Printf("Живых %d из %d\n", valid_counter, counter)
	sort.Slice(valid_sl, func(i, j int) bool {
		return valid_sl[i].Latency < valid_sl[j].Latency
	})
	valid_file, err := os.Create("valid_proxies.txt")
	if err != nil {
		fmt.Println("Ошибка создания файла")
		return
	}
	defer valid_file.Close()
	for _, proxy := range valid_sl {
		fmt.Fprintf(valid_file, "%s:%s:%s:%s\n",
			proxy.ip,
			proxy.port,
			proxy.login,
			proxy.password,
		)
	}
	fmt.Println("Прокси успешно записаны в файл: valid_proxies.txt")
}
